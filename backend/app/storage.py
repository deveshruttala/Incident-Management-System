"""All persistence in one module.

Layered as:

    Drivers      (PostgresPool, MongoConnection, RedisCache)
        – own the network connection lifecycle (connect / close)

    Repositories (SignalRepository, WorkItemRepository, RCARepository)
        – the *only* place SQL strings / Mongo / Redis commands live.
        – every write is wrapped in `with_retry` so transient failures
          don't lose data.

Why three different stores?
---------------------------
* MongoDB    — schemaless append log of every raw signal (the audit trail).
* PostgreSQL — transactional source-of-truth for WorkItem + RCA.
               FK from RCA → WorkItem enforces the "RCA exists" invariant.
* Redis      — sub-ms hot-path for the dashboard + per-minute time-series
               counters via simple HASHes.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime
from typing import Optional, TypedDict

import asyncpg
import redis.asyncio as aioredis
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase

from app.core import with_retry
from app.models import RCA, Signal, WorkItem, WorkItemStatus

log = logging.getLogger(__name__)


# =========================================================================== #
# Postgres                                                                    #
# =========================================================================== #
SCHEMA = """
CREATE TABLE IF NOT EXISTS work_items (
    work_item_id   TEXT        PRIMARY KEY,
    component_id   TEXT        NOT NULL,
    component_type TEXT        NOT NULL,
    severity       TEXT        NOT NULL,
    status         TEXT        NOT NULL,
    title          TEXT        NOT NULL,
    signal_count   INTEGER     NOT NULL DEFAULT 1,
    start_time     TIMESTAMPTZ NOT NULL,
    end_time       TIMESTAMPTZ,
    mttr_seconds   DOUBLE PRECISION,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS work_items_status_severity_idx
    ON work_items (status, severity);
CREATE INDEX IF NOT EXISTS work_items_component_idx
    ON work_items (component_id);

-- ON DELETE CASCADE: deleting a WI removes its RCA. The reverse direction
-- (RCA without WI) is impossible because of the FK — the database itself
-- enforces the "RCA refers to a real WorkItem" invariant.
CREATE TABLE IF NOT EXISTS rcas (
    work_item_id        TEXT        PRIMARY KEY REFERENCES work_items(work_item_id) ON DELETE CASCADE,
    incident_start      TIMESTAMPTZ NOT NULL,
    incident_end        TIMESTAMPTZ NOT NULL,
    root_cause_category TEXT        NOT NULL,
    fix_applied         TEXT        NOT NULL,
    prevention_steps    TEXT        NOT NULL,
    submitted_by        TEXT,
    submitted_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""


class PostgresPool:
    def __init__(self, dsn: str) -> None:
        self.dsn = dsn
        self.pool: Optional[asyncpg.Pool] = None

    async def connect(self) -> None:
        self.pool = await asyncpg.create_pool(
            self.dsn, min_size=2, max_size=20, command_timeout=10
        )
        async with self.pool.acquire() as conn:
            await conn.execute(SCHEMA)
        log.info("postgres connected and schema ensured")

    async def close(self) -> None:
        if self.pool is not None:
            await self.pool.close()


# =========================================================================== #
# MongoDB                                                                     #
# =========================================================================== #
class MongoConnection:
    def __init__(self, uri: str, db_name: str) -> None:
        self.uri = uri
        self.db_name = db_name
        self.client: Optional[AsyncIOMotorClient] = None
        self.db: Optional[AsyncIOMotorDatabase] = None

    async def connect(self) -> None:
        self.client = AsyncIOMotorClient(self.uri, serverSelectionTimeoutMS=5000)
        self.db = self.client[self.db_name]
        # Indexes for the two access patterns we actually use:
        #   1. "show me all signals for work_item X"   → work_item_id
        #   2. "what hit component C around time T"   → (component_id, received_at)
        await self.db.signals.create_index([("component_id", 1), ("received_at", -1)])
        await self.db.signals.create_index("work_item_id")
        await self.db.signals.create_index("received_at")
        log.info("mongo connected: db=%s", self.db_name)

    async def close(self) -> None:
        if self.client is not None:
            self.client.close()


# =========================================================================== #
# Redis (cache + per-minute time-series)                                      #
# =========================================================================== #
DASHBOARD_KEY = "ims:dashboard"        # HASH {work_item_id -> json}
TS_SIGNALS_KEY = "ims:ts:signals"      # HASH {minute_bucket_ts -> count}
TS_INCIDENTS_KEY = "ims:ts:incidents"  # HASH {minute_bucket_ts -> count}
TS_RETENTION_SECONDS = 24 * 3600


class RedisCache:
    def __init__(self, url: str) -> None:
        self.url = url
        self.client: Optional[aioredis.Redis] = None

    async def connect(self) -> None:
        self.client = aioredis.from_url(self.url, decode_responses=True)
        await self.client.ping()
        log.info("redis connected")

    async def close(self) -> None:
        if self.client is not None:
            await self.client.aclose()

    # ----- dashboard hot-path ---------------------------------------------
    async def upsert_dashboard(self, wi: WorkItem) -> None:
        if self.client is None:
            return
        await self.client.hset(DASHBOARD_KEY, wi.work_item_id, wi.model_dump_json())

    async def list_dashboard(self) -> list[dict]:
        if self.client is None:
            return []
        return [json.loads(r) for r in await self.client.hvals(DASHBOARD_KEY)]

    # ----- time-series ----------------------------------------------------
    async def record_signal(self, count: int = 1) -> None:
        await self._bump(TS_SIGNALS_KEY, count)

    async def record_incident(self, count: int = 1) -> None:
        await self._bump(TS_INCIDENTS_KEY, count)

    async def _bump(self, key: str, by: int) -> None:
        if self.client is None:
            return
        bucket = (int(time.time()) // 60) * 60  # minute granularity
        await self.client.hincrby(key, str(bucket), by)

    async def time_series(self, kind: str, since_seconds: int = 3600) -> list[dict]:
        if self.client is None:
            return []
        key = TS_SIGNALS_KEY if kind == "signals" else TS_INCIDENTS_KEY
        raw = await self.client.hgetall(key)
        cutoff = int(time.time()) - since_seconds
        result, stale = [], []
        for bucket_str, count_str in raw.items():
            bucket = int(bucket_str)
            if bucket < int(time.time()) - TS_RETENTION_SECONDS:
                stale.append(bucket_str)
                continue
            if bucket >= cutoff:
                result.append({"bucket_ts": bucket, "count": int(count_str)})
        if stale:
            await self.client.hdel(key, *stale)
        result.sort(key=lambda x: x["bucket_ts"])
        return result


# =========================================================================== #
# Repositories — the only places that translate domain ↔ storage              #
# =========================================================================== #
class SignalRepository:
    """Mongo-backed audit log of raw signal payloads."""

    def __init__(self, mongo: MongoConnection) -> None:
        self.mongo = mongo

    @property
    def collection(self):
        return self.mongo.db.signals

    async def insert(self, signal: Signal) -> None:
        async def _do():
            doc = signal.model_dump(mode="json")
            doc["_id"] = signal.signal_id
            await self.collection.insert_one(doc)
        await with_retry(_do, op_name="mongo_insert_signal")

    async def link_to_work_item(self, signal_id: str, work_item_id: str) -> None:
        async def _do():
            await self.collection.update_one(
                {"_id": signal_id}, {"$set": {"work_item_id": work_item_id}}
            )
        await with_retry(_do, op_name="mongo_link_signal")

    async def list_for_work_item(self, work_item_id: str, limit: int = 200) -> list[dict]:
        cursor = (
            self.collection.find({"work_item_id": work_item_id})
            .sort("received_at", -1)
            .limit(limit)
        )
        return [d async for d in cursor]

    async def search(
        self,
        component_id: Optional[str] = None,
        component_type: Optional[str] = None,
        error_code: Optional[str] = None,
        text: Optional[str] = None,
        limit: int = 100,
    ) -> list[dict]:
        """Free-form query over the data lake.

        Demonstrates the "Hint: think how this can be queried" requirement —
        the schemaless raw-payload store is searchable by any of the indexed
        fields, plus a substring match on the message body.
        """
        q: dict = {}
        if component_id:
            q["component_id"] = component_id
        if component_type:
            q["component_type"] = component_type
        if error_code:
            q["error_code"] = error_code
        if text:
            q["message"] = {"$regex": text, "$options": "i"}
        cursor = self.collection.find(q).sort("received_at", -1).limit(limit)
        return [d async for d in cursor]


class WorkItemStatsDict(TypedDict):
    """Shape returned by WorkItemRepository.stats() — typed so callers
    (including the dashboard JSON endpoint) get autocomplete and never
    hit a typo on a key name."""

    by_status: dict[str, int]
    avg_mttr_seconds: float
    closed_count: int


class WorkItemRepository:
    """Postgres-backed source of truth for WorkItem records."""

    def __init__(self, postgres: PostgresPool) -> None:
        self.postgres = postgres

    async def insert(self, wi: WorkItem) -> None:
        async def _do():
            async with self.postgres.pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO work_items (
                        work_item_id, component_id, component_type, severity,
                        status, title, signal_count, start_time, end_time,
                        mttr_seconds, created_at, updated_at
                    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
                    """,
                    wi.work_item_id, wi.component_id, wi.component_type,
                    wi.severity.value, wi.status.value, wi.title,
                    wi.signal_count, wi.start_time, wi.end_time,
                    wi.mttr_seconds, wi.created_at, wi.updated_at,
                )
        await with_retry(_do, op_name="pg_insert_work_item")

    async def bump_signal_count(self, work_item_id: str) -> None:
        async def _do():
            async with self.postgres.pool.acquire() as conn:
                await conn.execute(
                    "UPDATE work_items SET signal_count = signal_count + 1, "
                    "updated_at = NOW() WHERE work_item_id = $1",
                    work_item_id,
                )
        await with_retry(_do, op_name="pg_bump_signal_count")

    async def update_status(self, wi: WorkItem) -> None:
        async def _do():
            async with self.postgres.pool.acquire() as conn:
                async with conn.transaction():  # transactional state changes
                    await conn.execute(
                        """
                        UPDATE work_items
                           SET status = $2, end_time = $3,
                               mttr_seconds = $4, updated_at = $5
                         WHERE work_item_id = $1
                        """,
                        wi.work_item_id, wi.status.value, wi.end_time,
                        wi.mttr_seconds, wi.updated_at,
                    )
        await with_retry(_do, op_name="pg_update_status")

    async def get(self, work_item_id: str) -> Optional[WorkItem]:
        async with self.postgres.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM work_items WHERE work_item_id = $1", work_item_id
            )
            return _row_to_work_item(row) if row else None

    async def list(self, status: Optional[str] = None, limit: int = 100) -> list[WorkItem]:
        async with self.postgres.pool.acquire() as conn:
            if status:
                rows = await conn.fetch(
                    "SELECT * FROM work_items WHERE status = $1 "
                    "ORDER BY severity ASC, created_at DESC LIMIT $2",
                    status, limit,
                )
            else:
                rows = await conn.fetch(
                    "SELECT * FROM work_items "
                    "ORDER BY severity ASC, created_at DESC LIMIT $1",
                    limit,
                )
            return [_row_to_work_item(r) for r in rows]

    async def stats(self) -> WorkItemStatsDict:
        async with self.postgres.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT status, COUNT(*) AS n FROM work_items GROUP BY status"
            )
            mttr_row = await conn.fetchrow(
                "SELECT AVG(mttr_seconds) AS avg_mttr, COUNT(*) AS closed_count "
                "FROM work_items WHERE mttr_seconds IS NOT NULL"
            )
            return WorkItemStatsDict(
                by_status={r["status"]: r["n"] for r in rows},
                avg_mttr_seconds=float(mttr_row["avg_mttr"]) if mttr_row["avg_mttr"] else 0.0,
                closed_count=int(mttr_row["closed_count"] or 0),
            )


class RCARepository:
    def __init__(self, postgres: PostgresPool) -> None:
        self.postgres = postgres

    async def upsert(self, rca: RCA) -> None:
        async def _do():
            async with self.postgres.pool.acquire() as conn:
                async with conn.transaction():
                    await conn.execute(
                        """
                        INSERT INTO rcas (
                            work_item_id, incident_start, incident_end,
                            root_cause_category, fix_applied, prevention_steps,
                            submitted_by, submitted_at
                        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                        ON CONFLICT (work_item_id) DO UPDATE SET
                            incident_start      = EXCLUDED.incident_start,
                            incident_end        = EXCLUDED.incident_end,
                            root_cause_category = EXCLUDED.root_cause_category,
                            fix_applied         = EXCLUDED.fix_applied,
                            prevention_steps    = EXCLUDED.prevention_steps,
                            submitted_by        = EXCLUDED.submitted_by,
                            submitted_at        = EXCLUDED.submitted_at
                        """,
                        rca.work_item_id, rca.incident_start, rca.incident_end,
                        rca.root_cause_category.value, rca.fix_applied,
                        rca.prevention_steps, rca.submitted_by,
                        rca.submitted_at or datetime.utcnow(),
                    )
        await with_retry(_do, op_name="pg_upsert_rca")

    async def get(self, work_item_id: str) -> Optional[RCA]:
        async with self.postgres.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM rcas WHERE work_item_id = $1", work_item_id
            )
            if not row:
                return None
            return RCA(
                work_item_id=row["work_item_id"],
                incident_start=row["incident_start"],
                incident_end=row["incident_end"],
                root_cause_category=row["root_cause_category"],
                fix_applied=row["fix_applied"],
                prevention_steps=row["prevention_steps"],
                submitted_by=row["submitted_by"],
                submitted_at=row["submitted_at"],
            )


def _row_to_work_item(row) -> WorkItem:
    return WorkItem(
        work_item_id=row["work_item_id"],
        component_id=row["component_id"],
        component_type=row["component_type"],
        severity=row["severity"],
        status=WorkItemStatus(row["status"]),
        title=row["title"],
        signal_count=row["signal_count"],
        start_time=row["start_time"],
        end_time=row["end_time"],
        mttr_seconds=row["mttr_seconds"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )
