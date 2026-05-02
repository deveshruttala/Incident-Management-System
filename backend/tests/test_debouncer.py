"""Debouncer behaviour: same-component collapse + race-free leader election."""

from __future__ import annotations

import asyncio
import itertools

import pytest

from app.core import Debouncer


def _make_factory(prefix: str = "wi"):
    """Returns a leader factory that hands out wi-1, wi-2, … on each call."""
    counter = itertools.count(1)

    async def factory() -> str:
        return f"{prefix}-{next(counter)}"

    return factory


@pytest.mark.asyncio
async def test_first_signal_is_leader():
    d = Debouncer(window_seconds=10, max_signals=100)
    wi_id, was_leader = await d.route("cache-01", _make_factory())
    assert was_leader is True
    assert wi_id == "wi-1"


@pytest.mark.asyncio
async def test_subsequent_signals_attach_to_existing_work_item():
    d = Debouncer(window_seconds=10, max_signals=100)
    factory = _make_factory()
    first_id, _ = await d.route("cache-01", factory)
    for _ in range(5):
        wi_id, was_leader = await d.route("cache-01", factory)
        assert was_leader is False
        assert wi_id == first_id


@pytest.mark.asyncio
async def test_window_expiry_starts_new_work_item():
    d = Debouncer(window_seconds=0, max_signals=100)
    factory = _make_factory()
    first_id, was_leader = await d.route("cache-01", factory)
    assert was_leader is True
    await asyncio.sleep(0.01)
    second_id, was_leader = await d.route("cache-01", factory)
    assert was_leader is True
    assert second_id != first_id


@pytest.mark.asyncio
async def test_max_signals_per_window_promotes_a_new_leader():
    d = Debouncer(window_seconds=10, max_signals=3)
    factory = _make_factory()
    first_id, _ = await d.route("cache-01", factory)
    # 2nd and 3rd signals attach to the same WI.
    for _ in range(2):
        wi_id, was_leader = await d.route("cache-01", factory)
        assert was_leader is False
        assert wi_id == first_id
    # 4th hit the cap → must be promoted to a new leader.
    new_id, was_leader = await d.route("cache-01", factory)
    assert was_leader is True
    assert new_id != first_id


@pytest.mark.asyncio
async def test_route_serializes_concurrent_leaders():
    """500 concurrent same-component signals must collapse to ONE WI.

    Without per-component locking, multiple workers each see "no window
    exists" and create duplicate WIs. This test pins down the fix.
    """
    d = Debouncer(window_seconds=10, max_signals=10_000)
    leader_calls = 0

    async def leader_factory() -> str:
        nonlocal leader_calls
        await asyncio.sleep(0.005)  # widen the race window with a slow "DB write"
        leader_calls += 1
        return f"wi-{leader_calls}"

    results = await asyncio.gather(
        *(d.route("rdbms-primary", leader_factory) for _ in range(500))
    )
    leader_count = sum(1 for _, was_leader in results if was_leader)
    unique_ids = {wi for wi, _ in results}
    assert leader_count == 1
    assert unique_ids == {"wi-1"}


@pytest.mark.asyncio
async def test_route_parallelizes_across_components():
    """Different component_ids must not block each other."""
    d = Debouncer(window_seconds=10, max_signals=100)

    def slow(name: str):
        async def f() -> str:
            await asyncio.sleep(0.05)
            return f"wi-{name}"
        return f

    t = asyncio.get_event_loop().time()
    await asyncio.gather(d.route("a", slow("a")), d.route("b", slow("b")))
    elapsed = asyncio.get_event_loop().time() - t
    assert elapsed < 0.09, f"components should run in parallel, elapsed={elapsed:.3f}s"
