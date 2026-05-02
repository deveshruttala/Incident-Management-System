"""Notification store + AlertStrategy → sink integration."""

from __future__ import annotations

import pytest

from app.models import ComponentType, Severity, WorkItem, WorkItemStatus
from app.notifications import NotificationStore
from app.workflow import P0CriticalStrategy, P2MediumStrategy, select_strategy


def _wi(severity: Severity = Severity.P0) -> WorkItem:
    return WorkItem(
        component_id="rdbms-primary",
        component_type=ComponentType.RDBMS.value,
        severity=severity,
        status=WorkItemStatus.OPEN,
        title="RDBMS failure on rdbms-primary",
    )


@pytest.mark.asyncio
async def test_store_starts_empty():
    store = NotificationStore()
    assert await store.list() == []
    assert await store.unread_count() == 0


@pytest.mark.asyncio
async def test_alert_strategy_pushes_to_sink():
    store = NotificationStore()
    wi = _wi()
    await P0CriticalStrategy().fire(wi, store)

    notifications = await store.list()
    assert len(notifications) == 1
    n = notifications[0]
    assert n["severity"] == "P0"
    assert n["channel"] == "PagerDuty (on-call)"
    assert n["component_type"] == "RDBMS"
    assert n["work_item_id"] == wi.work_item_id
    assert n["read"] is False


@pytest.mark.asyncio
async def test_strategies_use_their_distinct_channels():
    store = NotificationStore()
    await P0CriticalStrategy().fire(_wi(Severity.P0), store)
    await P2MediumStrategy().fire(_wi(Severity.P2), store)
    notifications = await store.list()
    channels = {n["channel"] for n in notifications}
    assert channels == {"PagerDuty (on-call)", "Slack #ops"}


@pytest.mark.asyncio
async def test_unread_count_and_ack_one():
    store = NotificationStore()
    await P0CriticalStrategy().fire(_wi(), store)
    await P2MediumStrategy().fire(_wi(Severity.P2), store)
    assert await store.unread_count() == 2

    [first, _] = await store.list()
    assert await store.ack(first["id"]) is True
    assert await store.unread_count() == 1
    # Ack of unknown id returns False, no exception.
    assert await store.ack("nonexistent") is False


@pytest.mark.asyncio
async def test_ack_all_marks_everything_read():
    store = NotificationStore()
    for _ in range(5):
        await P0CriticalStrategy().fire(_wi(), store)
    assert await store.unread_count() == 5
    n_marked = await store.ack_all()
    assert n_marked == 5
    assert await store.unread_count() == 0


@pytest.mark.asyncio
async def test_unread_filter_only_returns_unread():
    store = NotificationStore()
    await P0CriticalStrategy().fire(_wi(), store)
    await P2MediumStrategy().fire(_wi(Severity.P2), store)
    await store.ack_all()
    await P0CriticalStrategy().fire(_wi(), store)  # one fresh
    unread = await store.list(unread_only=True)
    assert len(unread) == 1
    assert unread[0]["read"] is False


@pytest.mark.asyncio
async def test_buffer_is_bounded():
    store = NotificationStore(maxlen=3)
    for i in range(10):
        await P0CriticalStrategy().fire(_wi(), store)
    notifications = await store.list()
    # Only the 3 most recent should survive — the rest are dropped silently.
    assert len(notifications) == 3


@pytest.mark.asyncio
async def test_strategy_selection_dispatches_correctly():
    """The Strategy Pattern dispatch table maps each component type to its
    canonical severity. This pins the alerting policy down."""
    cases = [
        (ComponentType.RDBMS, Severity.P0),
        (ComponentType.MCP_HOST, Severity.P0),
        (ComponentType.API, Severity.P1),
        (ComponentType.QUEUE, Severity.P1),
        (ComponentType.CACHE, Severity.P2),
        (ComponentType.NOSQL, Severity.P2),
        (ComponentType.OTHER, Severity.P3),
    ]
    for comp, expected in cases:
        assert select_strategy(comp).severity == expected


@pytest.mark.asyncio
async def test_clear_drops_everything():
    store = NotificationStore()
    for _ in range(3):
        await P0CriticalStrategy().fire(_wi(), store)
    await store.clear()
    assert await store.list() == []
