"""In-app notification centre — replaces the log-only alert stubs.

What this gives us
------------------
The Strategy Pattern in `workflow.py` previously logged "would have paged
on-call" lines. That demonstrated the pattern but produced no visible
output anywhere a human looks. This module wires those alerts into a
**real notification channel** that the dashboard can render — a small
in-process store with REST endpoints + a WebSocket fan-out for live push.

Design decisions
----------------
1. **In-memory ring buffer.** Notifications are operationally noisy —
   we don't need them to survive a restart, and we don't want them
   accumulating forever. A bounded `deque(maxlen=200)` keeps the most
   recent 200 in O(1) push / pop.

2. **Sink is a Protocol, not a class.** Strategies talk to the
   `NotificationSink` Protocol so any implementation (in-memory, Redis,
   webhook, real PagerDuty client) can be swapped in without touching
   the strategies themselves.

3. **TypedDict on the wire.** The notification shape is stable and
   structural — `TypedDict` gives us editor autocomplete + JSON-friendly
   serialisation without the overhead of a Pydantic model.
"""

from __future__ import annotations

import asyncio
import time
from collections import deque
from typing import Iterable, Protocol, TypedDict
from uuid import uuid4


# --------------------------------------------------------------------------- #
# Wire shape — what /notifications returns and what the bell icon renders.    #
# --------------------------------------------------------------------------- #
class NotificationDict(TypedDict):
    """JSON-serialisable shape of a single notification entry."""
    id: str                  # opaque uuid hex; used by /ack endpoint
    severity: str            # "P0" | "P1" | "P2" | "P3"
    component_type: str
    component_id: str
    title: str               # short, human readable
    channel: str             # "PagerDuty" | "On-call" | "Slack #ops" | "Email"
    work_item_id: str
    timestamp: float         # unix epoch, seconds (float for ms precision)
    read: bool


# --------------------------------------------------------------------------- #
# Sink — what AlertStrategy implementations talk to.                          #
# --------------------------------------------------------------------------- #
class NotificationSink(Protocol):
    """Anything that can accept a notification.

    Today there's a single in-memory implementation; tomorrow you'd add a
    `RedisStreamSink`, a `PagerDutySink`, etc. The strategies never change.
    """

    def push(self, notification: NotificationDict) -> None: ...


# --------------------------------------------------------------------------- #
# In-memory implementation                                                    #
# --------------------------------------------------------------------------- #
class NotificationStore:
    """Bounded in-process notification buffer + WebSocket fan-out.

    Methods are intentionally small — push, list (with optional unread
    filter), ack one, ack all. The store does NOT live in Redis: it's
    operational telemetry, not source-of-truth data.
    """

    DEFAULT_MAX = 200

    def __init__(self, *, maxlen: int = DEFAULT_MAX, broadcaster=None) -> None:
        self._buf: deque[NotificationDict] = deque(maxlen=maxlen)
        # Single lock — all operations are tiny, contention is non-existent.
        self._lock = asyncio.Lock()
        self._broadcaster = broadcaster

    # ---- write side --------------------------------------------------------
    def push(self, notification: NotificationDict) -> None:
        # No `await` here — keeps the AlertStrategy.fire() path non-async-
        # contended. We schedule the WebSocket broadcast in the background.
        self._buf.appendleft(notification)
        if self._broadcaster is not None:
            try:
                asyncio.get_event_loop().create_task(
                    self._broadcaster.broadcast(
                        {"event": "notification", "data": notification}
                    )
                )
            except RuntimeError:
                # No running loop (e.g. during sync unit tests) — silently
                # skip the broadcast. The store still has the entry.
                pass

    def make(
        self,
        *,
        severity: str,
        component_type: str,
        component_id: str,
        title: str,
        channel: str,
        work_item_id: str,
    ) -> NotificationDict:
        """Factory that fills in id / timestamp / read."""
        return NotificationDict(
            id=uuid4().hex,
            severity=severity,
            component_type=component_type,
            component_id=component_id,
            title=title,
            channel=channel,
            work_item_id=work_item_id,
            timestamp=time.time(),
            read=False,
        )

    # ---- read side ---------------------------------------------------------
    async def list(self, *, limit: int = 50, unread_only: bool = False) -> list[NotificationDict]:
        async with self._lock:
            iterable: Iterable[NotificationDict] = self._buf
            if unread_only:
                iterable = (n for n in self._buf if not n["read"])
            return list(iterable)[:limit]

    async def unread_count(self) -> int:
        async with self._lock:
            return sum(1 for n in self._buf if not n["read"])

    async def ack(self, notification_id: str) -> bool:
        async with self._lock:
            for n in self._buf:
                if n["id"] == notification_id:
                    n["read"] = True
                    return True
            return False

    async def ack_all(self) -> int:
        async with self._lock:
            n_marked = 0
            for n in self._buf:
                if not n["read"]:
                    n["read"] = True
                    n_marked += 1
            return n_marked

    async def clear(self) -> None:
        """Drop everything — useful in tests + a 'clear all' UI button."""
        async with self._lock:
            self._buf.clear()
