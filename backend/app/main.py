"""FastAPI entrypoint. Wires storage + queue + workers + routes."""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware

from app.config import settings
from app.core import Debouncer, SignalQueue, TokenBucket, metrics
from app.notifications import NotificationStore
from app.routes import (
    SignalProcessor,
    broadcaster,
    incidents_router,
    ingest_router,
    notifications_router,
    ops_router,
    signals_router,
    ws_router,
)
from app.security import BodySizeLimitMiddleware, CorrelationIdMiddleware
from app.storage import (
    MongoConnection,
    PostgresPool,
    RCARepository,
    RedisCache,
    SignalRepository,
    WorkItemRepository,
)
from app.workflow import WorkflowEngine

logging.basicConfig(
    level=settings.log_level,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
log = logging.getLogger("ims")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Build the whole dependency graph on startup, tear it down on shutdown."""
    log.info("starting %s", settings.app_name)

    # 1. Storage drivers — connect in parallel for fast cold-start.
    postgres = PostgresPool(settings.postgres_dsn)
    mongo = MongoConnection(settings.mongo_uri, settings.mongo_db)
    cache = RedisCache(settings.redis_url)
    await asyncio.gather(postgres.connect(), mongo.connect(), cache.connect())

    # 2. Repositories sit on top of the drivers.
    work_item_repo = WorkItemRepository(postgres)
    rca_repo = RCARepository(postgres)
    signal_repo = SignalRepository(mongo)

    # 3. In-process primitives.
    queue = SignalQueue(maxsize=settings.queue_max_size)
    rate_limiter = TokenBucket(
        rate_per_second=settings.rate_limit_per_second,
        burst=settings.rate_limit_burst,
    )
    debouncer = Debouncer(
        window_seconds=settings.debounce_window_seconds,
        max_signals=settings.debounce_max_signals,
    )
    # Notification store — receives alerts from AlertStrategy.fire() and
    # fans them out to the dashboard's bell icon (in-memory + WebSocket).
    notification_store = NotificationStore(broadcaster=broadcaster)

    engine = WorkflowEngine(
        work_item_repo=work_item_repo,
        rca_repo=rca_repo,
        cache=cache,
        broadcaster=broadcaster,
        notification_sink=notification_store,
    )
    processor = SignalProcessor(
        queue=queue,
        engine=engine,
        signals_repo=signal_repo,
        cache=cache,
        debouncer=debouncer,
        worker_count=settings.worker_count,
    )

    # 4. Publish handles to app.state for the route handlers.
    app.state.postgres = postgres
    app.state.mongo = mongo
    app.state.cache = cache
    app.state.work_item_repo = work_item_repo
    app.state.rca_repo = rca_repo
    app.state.signal_repo = signal_repo
    app.state.queue = queue
    app.state.rate_limiter = rate_limiter
    app.state.engine = engine
    app.state.notification_store = notification_store

    # 5. Background tasks: workers + 5-second throughput printer + debouncer GC.
    await processor.start()
    metrics_task = asyncio.create_task(
        metrics.run_printer(settings.metrics_print_interval_seconds)
    )
    gc_task = asyncio.create_task(debouncer.gc_loop(interval=5.0))

    log.info("startup complete — listening")
    try:
        yield
    finally:
        log.info("shutting down …")
        metrics_task.cancel()
        gc_task.cancel()
        await processor.stop()
        await asyncio.gather(postgres.close(), mongo.close(), cache.close())


def create_app() -> FastAPI:
    app = FastAPI(
        title="Incident Management System",
        version="1.0.0",
        lifespan=lifespan,
        description="Mission-critical IMS — Zeotap SRE assignment",
    )
    # ---- Middleware (order matters: outer-most wraps the rest) ------------
    # CORS — controlled by env var. Defaults to "*" for local dev convenience;
    # set CORS_ALLOW_ORIGINS=https://ims.your-domain in production.
    origins = [o.strip() for o in settings.cors_allow_origins.split(",") if o.strip()]
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins or ["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )
    # GZip — significant payload savings on /incidents (JSON list endpoint)
    app.add_middleware(GZipMiddleware, minimum_size=1024)
    # Body-size cap — defence in depth against memory abuse
    app.add_middleware(BodySizeLimitMiddleware, max_bytes=settings.max_request_body_bytes)
    # Correlation IDs — every request gets X-Request-ID + structured access log
    app.add_middleware(CorrelationIdMiddleware)

    app.include_router(ops_router)
    app.include_router(ingest_router)
    app.include_router(incidents_router)
    app.include_router(signals_router)
    app.include_router(notifications_router)
    app.include_router(ws_router)
    return app


app = create_app()
