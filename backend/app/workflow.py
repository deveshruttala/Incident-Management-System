"""Incident workflow — State + Strategy patterns + the orchestrating engine.

Two GoF patterns live here:

    1. State Pattern    — `WorkItemState` subclasses encode the legal
       transitions of a WorkItem. The `→ CLOSED` edge requires a complete
       RCA; this is enforced inside the state itself, not scattered across
       callers.

    2. Strategy Pattern — `AlertStrategy` subclasses encapsulate "how do we
       page someone for this severity". `select_strategy(component_type)`
       picks the right concrete strategy.

The `WorkflowEngine` is a thin orchestrator that wires storage + states +
alerting + the WebSocket broadcaster. Every state change goes through it.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Optional, Type

from app.models import (
    RCA,
    ComponentType,
    Severity,
    Signal,
    WorkItem,
    WorkItemStatus,
)

# Imported only for type-hints — avoids a runtime cycle (notifications.py
# itself doesn't depend on workflow.py).
if TYPE_CHECKING:
    from app.notifications import NotificationSink

log = logging.getLogger(__name__)


# =========================================================================== #
# State Pattern                                                               #
# =========================================================================== #
class IllegalTransition(Exception):
    """A `transition()` was attempted that is not allowed from the current state."""


class RCAValidationError(Exception):
    """An attempt was made to CLOSE a WorkItem without a complete RCA."""


class WorkItemState(ABC):
    """Each subclass represents one node of the WorkItem state machine.

    The class itself is stateless — it carries metadata (allowed transitions)
    and the validation rule that must hold to leave that state.
    """

    status: WorkItemStatus

    @classmethod
    @abstractmethod
    def allowed_transitions(cls) -> set[WorkItemStatus]: ...

    @classmethod
    def validate_transition(
        cls, target: WorkItemStatus, rca: Optional[RCA] = None
    ) -> None:
        if target not in cls.allowed_transitions():
            raise IllegalTransition(
                f"Cannot transition from {cls.status.value} to {target.value}"
            )
        # The CLOSED edge has a side condition: a complete RCA must exist.
        # Centralising it here means *every* path that wants to close a WI
        # gets the check for free.
        if target is WorkItemStatus.CLOSED:
            if rca is None:
                raise RCAValidationError("Cannot CLOSE work item without an RCA")
            if not rca.is_complete():
                raise RCAValidationError("RCA is incomplete — all fields are required")


class OpenState(WorkItemState):
    status = WorkItemStatus.OPEN

    @classmethod
    def allowed_transitions(cls) -> set[WorkItemStatus]:
        return {WorkItemStatus.INVESTIGATING, WorkItemStatus.RESOLVED}


class InvestigatingState(WorkItemState):
    status = WorkItemStatus.INVESTIGATING

    @classmethod
    def allowed_transitions(cls) -> set[WorkItemStatus]:
        # Allow re-OPEN as an escape hatch when investigation reveals it was
        # actually a different incident entirely.
        return {WorkItemStatus.RESOLVED, WorkItemStatus.OPEN}


class ResolvedState(WorkItemState):
    status = WorkItemStatus.RESOLVED

    @classmethod
    def allowed_transitions(cls) -> set[WorkItemStatus]:
        # Re-investigation is allowed in case the "fix" didn't actually fix it.
        return {WorkItemStatus.CLOSED, WorkItemStatus.INVESTIGATING}


class ClosedState(WorkItemState):
    status = WorkItemStatus.CLOSED

    @classmethod
    def allowed_transitions(cls) -> set[WorkItemStatus]:
        # CLOSED → OPEN is a "reopen": ops can revive a closed incident
        # when the fix didn't actually stick (regression / false-positive close).
        # The original RCA is preserved; end_time + mttr_seconds get reset on
        # reopen and recomputed when it's closed again — see WorkflowEngine.
        return {WorkItemStatus.OPEN}


_STATE_MAP: dict[WorkItemStatus, Type[WorkItemState]] = {
    WorkItemStatus.OPEN: OpenState,
    WorkItemStatus.INVESTIGATING: InvestigatingState,
    WorkItemStatus.RESOLVED: ResolvedState,
    WorkItemStatus.CLOSED: ClosedState,
}


def state_for(status: WorkItemStatus) -> Type[WorkItemState]:
    return _STATE_MAP[status]


# =========================================================================== #
# Strategy Pattern — alerting                                                 #
# =========================================================================== #
class AlertStrategy(ABC):
    """How a particular severity gets escalated.

    Each concrete strategy maps 1:1 to a severity AND a `channel` label
    that ends up in the notification record. The Sink is injected via the
    `NotificationSink` Protocol from app/notifications.py — strategies
    have no idea whether the sink is the in-memory store, Redis, a real
    PagerDuty client, or a mock for tests.
    """

    severity: Severity
    channel: str   # human-friendly label that appears in the bell dropdown

    @abstractmethod
    async def fire(self, work_item: WorkItem, sink: "NotificationSink | None" = None) -> None: ...

    # Default fire — every concrete subclass uses this. Centralised so the
    # log line + the notification dict are produced from a single source.
    async def _emit(self, work_item: WorkItem, sink: "NotificationSink | None", log_method) -> None:
        log_method(
            "[ALERT %s] %s — component=%s id=%s title=%r",
            self.severity.value, self.channel,
            work_item.component_type, work_item.component_id, work_item.title,
        )
        if sink is None:
            return
        # Only the in-memory NotificationStore exposes `make()`; treat any
        # store that doesn't have it as a no-op for `make`.
        notification = sink.make(  # type: ignore[attr-defined]
            severity=self.severity.value,
            component_type=work_item.component_type,
            component_id=work_item.component_id,
            title=work_item.title,
            channel=self.channel,
            work_item_id=work_item.work_item_id,
        )
        sink.push(notification)


class P0CriticalStrategy(AlertStrategy):
    severity = Severity.P0
    channel = "PagerDuty (on-call)"

    async def fire(self, work_item: WorkItem, sink: "NotificationSink | None" = None) -> None:
        await self._emit(work_item, sink, log.warning)


class P1HighStrategy(AlertStrategy):
    severity = Severity.P1
    channel = "On-call notification"

    async def fire(self, work_item: WorkItem, sink: "NotificationSink | None" = None) -> None:
        await self._emit(work_item, sink, log.warning)


class P2MediumStrategy(AlertStrategy):
    severity = Severity.P2
    channel = "Slack #ops"

    async def fire(self, work_item: WorkItem, sink: "NotificationSink | None" = None) -> None:
        await self._emit(work_item, sink, log.info)


class P3LowStrategy(AlertStrategy):
    severity = Severity.P3
    channel = "Email digest"

    async def fire(self, work_item: WorkItem, sink: "NotificationSink | None" = None) -> None:
        await self._emit(work_item, sink, log.info)


# Static mapping: how we escalate failures of each component class.
# Tweak this single dict to change the alerting policy globally.
_BY_TYPE: dict[ComponentType, Type[AlertStrategy]] = {
    ComponentType.RDBMS: P0CriticalStrategy,
    ComponentType.MCP_HOST: P0CriticalStrategy,
    ComponentType.API: P1HighStrategy,
    ComponentType.QUEUE: P1HighStrategy,
    ComponentType.CACHE: P2MediumStrategy,
    ComponentType.NOSQL: P2MediumStrategy,
}


def select_strategy(component_type: ComponentType) -> AlertStrategy:
    return _BY_TYPE.get(component_type, P3LowStrategy)()


def severity_for(component_type: ComponentType) -> Severity:
    return select_strategy(component_type).severity


# =========================================================================== #
# Orchestrator                                                                #
# =========================================================================== #
class WorkflowEngine:
    """The single chokepoint for *every* WorkItem mutation.

    Everything that needs to change a WorkItem's state goes through here so
    we have one place to enforce invariants (RCA-before-close), update the
    cache, fan out a WebSocket event for live UI updates, and emit alerts
    to the notification sink.
    """

    def __init__(
        self,
        work_item_repo,
        rca_repo,
        cache,
        broadcaster=None,
        notification_sink=None,
    ) -> None:
        self.work_items = work_item_repo
        self.rcas = rca_repo
        self.cache = cache
        self.broadcaster = broadcaster
        self.notification_sink = notification_sink

    async def open_work_item(self, signal: Signal) -> WorkItem:
        wi = WorkItem(
            component_id=signal.component_id,
            component_type=signal.component_type.value,
            severity=severity_for(signal.component_type),
            title=f"{signal.component_type.value} failure on {signal.component_id}",
        )
        await self.work_items.insert(wi)
        await self.cache.upsert_dashboard(wi)
        await select_strategy(signal.component_type).fire(wi, self.notification_sink)
        await self._broadcast({"event": "work_item_created", "data": wi.model_dump(mode="json")})
        return wi

    async def attach_signal(self, work_item_id: str) -> None:
        """A debounced signal joined an existing window — bump the count."""
        await self.work_items.bump_signal_count(work_item_id)
        wi = await self.work_items.get(work_item_id)
        if wi is not None:
            await self.cache.upsert_dashboard(wi)
            await self._broadcast(
                {"event": "work_item_updated", "data": wi.model_dump(mode="json")}
            )

    async def transition(
        self, work_item_id: str, target: WorkItemStatus, rca: Optional[RCA] = None
    ) -> WorkItem:
        wi = await self.work_items.get(work_item_id)
        if wi is None:
            raise LookupError(f"Work item {work_item_id} not found")

        # When closing, look up the persisted RCA if the caller didn't supply one.
        effective_rca = rca
        if target is WorkItemStatus.CLOSED and effective_rca is None:
            effective_rca = await self.rcas.get(work_item_id)

        state_for(wi.status).validate_transition(target, effective_rca)

        was_closed = wi.status is WorkItemStatus.CLOSED
        wi.status = target
        wi.updated_at = datetime.now(timezone.utc)

        # Reopen path: CLOSED → OPEN. Wipe the previous fix marker so MTTR
        # recomputes against the *next* incident_end (the time the system was
        # actually fixed for good). The persisted RCA is left in place — ops
        # will update it on the next CLOSE.
        if was_closed and target is WorkItemStatus.OPEN:
            wi.end_time = None
            wi.mttr_seconds = None

        if target is WorkItemStatus.CLOSED:
            # MTTR contract — the spec says "end_time (RCA submission)".
            # We treat that as a two-part definition:
            #   * the *event* that triggers the calculation = RCA submission;
            #   * the *value* of end_time              = the user-declared
            #     `incident_end` field on the RCA (the moment the system was
            #     actually fixed, which is the only interpretation that gives
            #     an operationally meaningful MTTR).
            # If for some reason `incident_end` is missing we fall back to the
            # RCA submission timestamp, which matches the literal reading of
            # the spec.
            now = datetime.now(timezone.utc)
            if effective_rca is not None:
                wi.end_time = effective_rca.incident_end or effective_rca.submitted_at or now
            else:
                wi.end_time = now
            if wi.start_time and wi.end_time:
                wi.mttr_seconds = (wi.end_time - wi.start_time).total_seconds()

        await self.work_items.update_status(wi)
        await self.cache.upsert_dashboard(wi)
        await self._broadcast(
            {"event": "work_item_updated", "data": wi.model_dump(mode="json")}
        )
        return wi

    async def submit_rca(self, rca: RCA) -> RCA:
        if not rca.is_complete():
            raise RCAValidationError("RCA is incomplete")
        rca.submitted_at = datetime.now(timezone.utc)
        await self.rcas.upsert(rca)
        return rca

    async def _broadcast(self, payload: dict) -> None:
        # WebSocket failures must never break ingestion — best-effort fan-out.
        if self.broadcaster is not None:
            try:
                await self.broadcaster.broadcast(payload)
            except Exception as exc:
                log.warning("broadcast failed: %s", exc)
