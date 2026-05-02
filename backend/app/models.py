"""Pydantic data models for the IMS.

All domain types live here:
    Signal      — raw error/latency event arriving at /ingest
    WorkItem    — structured incident record persisted in Postgres
    RCA         — Root Cause Analysis (required to CLOSE a WorkItem)

The models double as the wire schema (FastAPI auto-validates them) and as
the in-memory representation used by the workers and storage layers.
"""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional
from uuid import uuid4

from pydantic import BaseModel, Field, field_validator


# --------------------------------------------------------------------------- #
# Enumerations                                                                #
# --------------------------------------------------------------------------- #
class ComponentType(str, Enum):
    """The kind of system component a signal originates from.

    The component type drives the alerting Strategy (see workflow.py):
    e.g. RDBMS / MCP_HOST → P0 (page on-call), CACHE → P2 (Slack).
    """
    API = "API"
    MCP_HOST = "MCP_HOST"
    CACHE = "CACHE"
    QUEUE = "QUEUE"
    RDBMS = "RDBMS"
    NOSQL = "NOSQL"
    OTHER = "OTHER"


class WorkItemStatus(str, Enum):
    OPEN = "OPEN"
    INVESTIGATING = "INVESTIGATING"
    RESOLVED = "RESOLVED"
    CLOSED = "CLOSED"


class Severity(str, Enum):
    P0 = "P0"
    P1 = "P1"
    P2 = "P2"
    P3 = "P3"


class RootCauseCategory(str, Enum):
    INFRASTRUCTURE = "INFRASTRUCTURE"
    DEPLOYMENT = "DEPLOYMENT"
    CONFIG_CHANGE = "CONFIG_CHANGE"
    DEPENDENCY_FAILURE = "DEPENDENCY_FAILURE"
    CODE_DEFECT = "CODE_DEFECT"
    CAPACITY = "CAPACITY"
    HUMAN_ERROR = "HUMAN_ERROR"
    UNKNOWN = "UNKNOWN"


# --------------------------------------------------------------------------- #
# Signal                                                                      #
# --------------------------------------------------------------------------- #
class SignalIn(BaseModel):
    """The exact JSON payload accepted by `POST /ingest`."""

    component_id: str = Field(..., min_length=1, max_length=128)
    component_type: ComponentType = ComponentType.OTHER
    message: str = Field(..., min_length=1, max_length=2048)
    latency_ms: Optional[float] = None
    error_code: Optional[str] = None
    payload: dict[str, Any] = Field(default_factory=dict)
    occurred_at: Optional[datetime] = None


class Signal(SignalIn):
    """A signal after server-side enrichment, ready to persist."""

    signal_id: str = Field(default_factory=lambda: uuid4().hex)
    received_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    work_item_id: Optional[str] = None  # filled in once the worker assigns one

    @classmethod
    def from_input(cls, payload: SignalIn) -> "Signal":
        data = payload.model_dump()
        if data.get("occurred_at") is None:
            data["occurred_at"] = datetime.now(timezone.utc)
        return cls(**data)


# --------------------------------------------------------------------------- #
# WorkItem                                                                    #
# --------------------------------------------------------------------------- #
class WorkItem(BaseModel):
    """The structured incident record. One row per row in Postgres."""

    work_item_id: str = Field(default_factory=lambda: uuid4().hex)
    component_id: str
    component_type: str
    severity: Severity
    status: WorkItemStatus = WorkItemStatus.OPEN
    title: str
    signal_count: int = 1
    start_time: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    end_time: Optional[datetime] = None
    mttr_seconds: Optional[float] = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


# --------------------------------------------------------------------------- #
# RCA                                                                         #
# --------------------------------------------------------------------------- #
class RCA(BaseModel):
    """Root Cause Analysis record. Required to close a WorkItem.

    `is_complete()` is the single source of truth for "is this RCA acceptable
    to allow a CLOSED transition" — referenced by the State Pattern in
    `workflow.py`.
    """

    work_item_id: str
    incident_start: datetime
    incident_end: datetime
    root_cause_category: RootCauseCategory
    fix_applied: str = Field(..., min_length=10)
    prevention_steps: str = Field(..., min_length=10)
    submitted_by: Optional[str] = None
    submitted_at: Optional[datetime] = None

    @field_validator("incident_end")
    @classmethod
    def _end_after_start(cls, v: datetime, info) -> datetime:
        start = info.data.get("incident_start")
        if start and v <= start:
            raise ValueError("incident_end must be after incident_start")
        return v

    def is_complete(self) -> bool:
        return bool(
            self.work_item_id
            and self.incident_start
            and self.incident_end
            and self.root_cause_category
            and self.fix_applied and len(self.fix_applied.strip()) >= 10
            and self.prevention_steps and len(self.prevention_steps.strip()) >= 10
        )
