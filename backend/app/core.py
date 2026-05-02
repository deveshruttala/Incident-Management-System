"""Core in-process primitives.

This single module owns everything that runs **inside** the Python process and
has no external dependencies (no DB drivers, no HTTP):

    SignalQueue   — bounded asyncio queue providing backpressure (raises
                    QueueFull → 429 at the HTTP edge instead of OOMing)
    TokenBucket   — per-key token-bucket rate limiter
    Debouncer     — collapses N signals for the same component_id into one
                    WorkItem within a sliding window (race-free per-component)
    Metrics       — counters / gauges + a 5-second console throughput printer
    with_retry    — tenacity-backed retry helper for storage writes
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict
from dataclasses import dataclass
from threading import Lock
from typing import Awaitable, Callable, Tuple, TypeVar

from tenacity import (
    AsyncRetrying,
    RetryError,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from app.models import Signal

log = logging.getLogger(__name__)
T = TypeVar("T")


# =========================================================================== #
# 1. Bounded queue — the heart of our backpressure story                      #
# =========================================================================== #
class QueueFull(Exception):
    """Raised when the in-process queue cannot accept more signals.

    The HTTP layer translates this to `429 Too Many Requests` with a
    `Retry-After` header so producers learn to slow down via standard HTTP.
    """


class SignalQueue:
    """A bounded asyncio.Queue with a non-blocking `offer`.

    Why bounded?  At 10k signals/s a Python process holding an unbounded
    queue can OOM in seconds when the persistence layer slows down. Capping
    the queue means the *worst* failure mode is rejecting traffic — never
    crashing.
    """

    def __init__(self, maxsize: int) -> None:
        self._queue: asyncio.Queue[Signal] = asyncio.Queue(maxsize=maxsize)
        self._maxsize = maxsize

    @property
    def maxsize(self) -> int:
        return self._maxsize

    def qsize(self) -> int:
        return self._queue.qsize()

    def offer(self, signal: Signal) -> None:
        """Non-blocking enqueue. Raises QueueFull instead of awaiting."""
        try:
            self._queue.put_nowait(signal)
        except asyncio.QueueFull as exc:
            raise QueueFull() from exc

    async def get(self) -> Signal:
        return await self._queue.get()

    def task_done(self) -> None:
        self._queue.task_done()


# =========================================================================== #
# 2. Token-bucket rate limiter                                                #
# =========================================================================== #
class TokenBucket:
    """Classic token bucket keyed by an arbitrary string (e.g. client IP).

    Used as the first defence at `/ingest` so a single misbehaving producer
    cannot starve the whole queue.
    """

    def __init__(self, rate_per_second: float, burst: int) -> None:
        self._rate = float(rate_per_second)
        self._capacity = float(burst)
        # state per key: (tokens_available, last_refill_monotonic)
        self._buckets: dict[str, tuple[float, float]] = defaultdict(
            lambda: (self._capacity, time.monotonic())
        )
        self._lock = Lock()

    def allow(self, key: str, cost: float = 1.0) -> bool:
        with self._lock:
            tokens, last = self._buckets[key]
            now = time.monotonic()
            tokens = min(self._capacity, tokens + (now - last) * self._rate)
            if tokens >= cost:
                self._buckets[key] = (tokens - cost, now)
                return True
            self._buckets[key] = (tokens, now)
            return False


# =========================================================================== #
# 3. Debouncer                                                                #
# =========================================================================== #
@dataclass
class _DebounceWindow:
    work_item_id: str
    opened_at: float
    signal_count: int


class Debouncer:
    """Collapse signals-for-the-same-component into a single WorkItem.

    Concurrency design
    ------------------
    A naïve "check then create" implementation has a race: 8 workers all
    seeing "no window exists" can each create a WorkItem for the same
    component before any of them registers their result.

    We use a **per-component asyncio.Lock** so:
      * Same component_id → fully serialized (always exactly one WI / window)
      * Different component_ids → fully parallel (no global bottleneck)
    """

    def __init__(self, window_seconds: int, max_signals: int) -> None:
        self._window = float(window_seconds)
        self._max_signals = max_signals
        self._windows: dict[str, _DebounceWindow] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._registry_lock = asyncio.Lock()

    async def _lock_for(self, component_id: str) -> asyncio.Lock:
        async with self._registry_lock:
            lock = self._locks.get(component_id)
            if lock is None:
                lock = asyncio.Lock()
                self._locks[component_id] = lock
            return lock

    async def route(
        self,
        component_id: str,
        leader_factory: Callable[[], Awaitable[str]],
    ) -> Tuple[str, bool]:
        """Atomically decide leader vs follower for an incoming signal.

        * If an open window exists → returns `(existing_wi_id, False)`.
        * Otherwise → calls `leader_factory()` to *create* a new WorkItem
          while still holding the per-component lock, stores the result
          under a fresh window, and returns `(new_wi_id, True)`.
        """
        lock = await self._lock_for(component_id)
        async with lock:
            now = time.monotonic()
            window = self._windows.get(component_id)
            if (
                window
                and (now - window.opened_at) <= self._window
                and window.signal_count < self._max_signals
            ):
                window.signal_count += 1
                return window.work_item_id, False

            new_id = await leader_factory()
            self._windows[component_id] = _DebounceWindow(
                work_item_id=new_id, opened_at=now, signal_count=1
            )
            return new_id, True

    # ----- background GC --------------------------------------------------
    async def gc_loop(self, interval: float = 5.0) -> None:
        """Drop expired windows. Lock objects are kept (cheap, avoids churn)."""
        while True:
            await asyncio.sleep(interval)
            async with self._registry_lock:
                now = time.monotonic()
                stale = [
                    cid for cid, w in self._windows.items()
                    if (now - w.opened_at) > self._window
                ]
                for cid in stale:
                    self._windows.pop(cid, None)


# =========================================================================== #
# 4. Metrics                                                                  #
# =========================================================================== #
class Metrics:
    """In-process counters / gauges with a periodic stdout printer.

    The printer prints `[throughput]` lines every N seconds — required by the
    assignment ("print throughput metrics (Signals/sec) to the console every
    5 seconds"). Also exposes a Prometheus exposition for `/metrics`.
    """

    def __init__(self) -> None:
        self._counters: dict[str, int] = defaultdict(int)
        self._gauges: dict[str, float] = defaultdict(float)
        self._lock = Lock()
        self._started_at = time.monotonic()
        self._last_print_at = self._started_at
        self._last_counts: dict[str, int] = {}

    def incr(self, name: str, by: int = 1) -> None:
        with self._lock:
            self._counters[name] += by

    def gauge(self, name: str, value: float) -> None:
        with self._lock:
            self._gauges[name] = value

    def prometheus(self) -> str:
        lines: list[str] = []
        with self._lock:
            for k, v in self._counters.items():
                lines.append(f"# TYPE ims_{k} counter")
                lines.append(f"ims_{k} {v}")
            for k, v in self._gauges.items():
                lines.append(f"# TYPE ims_{k} gauge")
                lines.append(f"ims_{k} {v}")
            lines.append(f"ims_uptime_seconds {time.monotonic() - self._started_at:.2f}")
        return "\n".join(lines) + "\n"

    async def run_printer(self, interval_seconds: int) -> None:
        while True:
            await asyncio.sleep(interval_seconds)
            now = time.monotonic()
            elapsed = max(now - self._last_print_at, 0.001)
            with self._lock:
                snapshot = dict(self._counters)
                queue_depth = self._gauges.get("queue_depth", 0)
            d_in = snapshot.get("signals_ingested", 0) - self._last_counts.get("signals_ingested", 0)
            d_proc = snapshot.get("signals_processed", 0) - self._last_counts.get("signals_processed", 0)
            d_drop = snapshot.get("signals_dropped_backpressure", 0) - self._last_counts.get("signals_dropped_backpressure", 0)
            self._last_counts = snapshot
            self._last_print_at = now
            log.info(
                "[throughput] ingest=%d/s process=%d/s drop=%d/s queue=%d "
                "total_in=%d total_out=%d",
                int(d_in / elapsed), int(d_proc / elapsed), int(d_drop / elapsed),
                int(queue_depth),
                snapshot.get("signals_ingested", 0),
                snapshot.get("signals_processed", 0),
            )


# Module-wide singleton — every component imports the same instance.
metrics = Metrics()


# =========================================================================== #
# 5. Retry helper                                                             #
# =========================================================================== #
async def with_retry(
    coro_factory: Callable[[], Awaitable[T]],
    *,
    attempts: int = 3,
    op_name: str = "db_op",
    exceptions: tuple[type[BaseException], ...] = (Exception,),
) -> T:
    """Run an async operation with exponential-backoff retry.

    Takes a *factory* (zero-arg callable returning a fresh coroutine) rather
    than a coroutine — required because each retry needs a new awaitable.
    """
    try:
        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(attempts),
            wait=wait_exponential(multiplier=0.1, min=0.1, max=2.0),
            retry=retry_if_exception_type(exceptions),
            reraise=True,
        ):
            with attempt:
                return await coro_factory()
    except RetryError as exc:  # pragma: no cover — reraise=True flips this path
        log.error("[%s] giving up after %d attempts: %s", op_name, attempts, exc)
        raise
    raise RuntimeError("unreachable")  # pragma: no cover
