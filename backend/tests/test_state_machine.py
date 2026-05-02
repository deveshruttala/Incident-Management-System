"""Exhaustive transition matrix for the WorkItem state machine."""

from __future__ import annotations

import pytest

from app.models import WorkItemStatus
from app.workflow import (
    ClosedState,
    IllegalTransition,
    InvestigatingState,
    OpenState,
    ResolvedState,
)


@pytest.mark.parametrize(
    "src, allowed",
    [
        (OpenState,          {WorkItemStatus.INVESTIGATING, WorkItemStatus.RESOLVED}),
        (InvestigatingState, {WorkItemStatus.RESOLVED, WorkItemStatus.OPEN}),
        (ResolvedState,      {WorkItemStatus.CLOSED, WorkItemStatus.INVESTIGATING}),
        # CLOSED is no longer terminal — operators can reopen via CLOSED → OPEN.
        (ClosedState,        {WorkItemStatus.OPEN}),
    ],
)
def test_allowed_transitions_matrix(src, allowed):
    assert src.allowed_transitions() == allowed


def test_closed_can_only_transition_to_open():
    """Reopen is allowed; everything else from CLOSED is illegal."""
    ClosedState.validate_transition(WorkItemStatus.OPEN)  # ✓ legal
    for tgt in (
        WorkItemStatus.INVESTIGATING,
        WorkItemStatus.RESOLVED,
        WorkItemStatus.CLOSED,
    ):
        with pytest.raises(IllegalTransition):
            ClosedState.validate_transition(tgt)


def test_open_to_investigating_is_legal():
    OpenState.validate_transition(WorkItemStatus.INVESTIGATING)


def test_investigating_can_reopen():
    InvestigatingState.validate_transition(WorkItemStatus.OPEN)
