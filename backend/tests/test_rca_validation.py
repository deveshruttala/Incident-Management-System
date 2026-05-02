"""RCA validation: a WorkItem cannot be CLOSED without a complete RCA."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from app.models import RCA, RootCauseCategory, WorkItemStatus
from app.workflow import (
    IllegalTransition,
    OpenState,
    RCAValidationError,
    ResolvedState,
    state_for,
)


def _rca(complete: bool = True) -> RCA:
    """Helper to build a valid (or trivially-invalid) RCA."""
    return RCA(
        work_item_id="wi-1",
        incident_start=datetime.now(timezone.utc) - timedelta(minutes=30),
        incident_end=datetime.now(timezone.utc),
        root_cause_category=RootCauseCategory.INFRASTRUCTURE,
        fix_applied="Failed over the primary RDBMS to the standby replica" if complete else "x",
        prevention_steps="Add replica health monitoring + auto-failover" if complete else "y",
        submitted_by="oncall@zeotap",
    )


def test_close_without_rca_is_rejected():
    with pytest.raises(RCAValidationError):
        ResolvedState.validate_transition(WorkItemStatus.CLOSED, rca=None)


def test_close_with_incomplete_rca_is_rejected():
    # Pydantic itself will reject the too-short fields → ValueError
    with pytest.raises((RCAValidationError, ValueError)):
        bad = RCA(
            work_item_id="wi-1",
            incident_start=datetime.now(timezone.utc) - timedelta(minutes=5),
            incident_end=datetime.now(timezone.utc),
            root_cause_category=RootCauseCategory.UNKNOWN,
            fix_applied="short",
            prevention_steps="also short",
        )
        ResolvedState.validate_transition(WorkItemStatus.CLOSED, rca=bad)


def test_close_with_complete_rca_is_allowed():
    ResolvedState.validate_transition(WorkItemStatus.CLOSED, rca=_rca(complete=True))


def test_open_to_closed_is_illegal_even_with_rca():
    with pytest.raises(IllegalTransition):
        OpenState.validate_transition(WorkItemStatus.CLOSED, rca=_rca(complete=True))


def test_state_for_resolves_concrete_class():
    assert state_for(WorkItemStatus.OPEN) is OpenState
    assert state_for(WorkItemStatus.RESOLVED) is ResolvedState


def test_rca_end_must_be_after_start():
    with pytest.raises(ValueError):
        RCA(
            work_item_id="wi-1",
            incident_start=datetime.now(timezone.utc),
            incident_end=datetime.now(timezone.utc) - timedelta(minutes=1),
            root_cause_category=RootCauseCategory.INFRASTRUCTURE,
            fix_applied="rolled back the broken deploy",
            prevention_steps="add canary stage to pipeline",
        )


def test_is_complete_rejects_whitespace_only_fields():
    rca = _rca(complete=True)
    rca.fix_applied = "   " * 10  # bypass Pydantic by mutating after construct
    assert not rca.is_complete()
