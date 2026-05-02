"""HTTP/WebSocket routes + the worker pool that drains the queue.

These three things travel together because the API objects (Broadcaster,
SignalProcessor) plus the FastAPI routers all need the same `app.state`
wiring built in `main.py`.
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
from typing import Any, List, Optional

from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    Query,
    Request,
    Response,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel

from app.core import Debouncer, QueueFull, SignalQueue, metrics
from app.models import RCA, ComponentType, Signal, SignalIn, WorkItemStatus
from app.security import require_api_key
from app.workflow import IllegalTransition, RCAValidationError, WorkflowEngine

log = logging.getLogger(__name__)


# =========================================================================== #
# WebSocket fan-out                                                           #
# =========================================================================== #
class Broadcaster:
    """In-process pub-sub for `/ws` clients.

    Used by the WorkflowEngine to push real-time updates to the dashboard
    so the UI doesn't have to poll.
    """

    def __init__(self) -> None:
        self._clients: set[WebSocket] = set()
        self._lock = asyncio.Lock()

    async def add(self, ws: WebSocket) -> None:
        await ws.accept()
        async with self._lock:
            self._clients.add(ws)
        log.info("ws client connected (total=%d)", len(self._clients))

    async def remove(self, ws: WebSocket) -> None:
        async with self._lock:
            self._clients.discard(ws)

    async def broadcast(self, message: dict[str, Any]) -> None:
        payload = json.dumps(message, default=str)
        async with self._lock:
            dead: list[WebSocket] = []
            for ws in self._clients:
                try:
                    await ws.send_text(payload)
                except Exception:
                    dead.append(ws)
            for ws in dead:
                self._clients.discard(ws)


broadcaster = Broadcaster()


# =========================================================================== #
# Worker pool that drains the SignalQueue                                     #
# =========================================================================== #
class SignalProcessor:
    """N concurrent asyncio tasks pulling from the queue and processing."""

    def __init__(
        self,
        queue: SignalQueue,
        engine: WorkflowEngine,
        signals_repo,
        cache,
        debouncer: Debouncer,
        worker_count: int,
    ) -> None:
        self.queue = queue
        self.engine = engine
        self.signals = signals_repo
        self.cache = cache
        self.debouncer = debouncer
        self.worker_count = worker_count
        self._tasks: list[asyncio.Task] = []
        self._stopping = asyncio.Event()

    async def start(self) -> None:
        for i in range(self.worker_count):
            self._tasks.append(asyncio.create_task(self._run(i)))
        log.info("started %d signal processor workers", self.worker_count)

    async def stop(self) -> None:
        self._stopping.set()
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)

    async def _run(self, worker_id: int) -> None:
        while not self._stopping.is_set():
            try:
                signal = await self.queue.get()
            except asyncio.CancelledError:
                return
            try:
                await self._process(signal)
            except Exception as exc:
                # A failed signal must not kill the worker — log and move on.
                log.exception("worker=%d processing failed: %s", worker_id, exc)
                metrics.incr("signals_failed")
            finally:
                self.queue.task_done()
                metrics.gauge("queue_depth", self.queue.qsize())

    async def _process(self, signal: Signal) -> None:
        # Step 1 — always persist the raw payload to Mongo (the audit log).
        await self.signals.insert(signal)
        await self.cache.record_signal()

        # Step 2 — race-free leader election via per-component lock.
        # The factory closure is only invoked if this signal is the leader
        # for its component; the lock around the whole `route()` call
        # prevents concurrent workers from creating duplicate WIs.
        async def _create_work_item() -> str:
            wi = await self.engine.open_work_item(signal)
            await self.cache.record_incident()
            metrics.incr("incidents_created")
            return wi.work_item_id

        wi_id, was_leader = await self.debouncer.route(
            signal.component_id, _create_work_item
        )

        # Step 3 — link this signal to its WI in Mongo so the detail view
        # can show all the signals that contributed to an incident.
        await self.signals.link_to_work_item(signal.signal_id, wi_id)

        if not was_leader:
            await self.engine.attach_signal(wi_id)
            metrics.incr("signals_debounced")

        metrics.incr("signals_processed")


# =========================================================================== #
# Routers                                                                     #
# =========================================================================== #
ops_router = APIRouter(tags=["ops"])
# `require_api_key` is enforced for the entire ingest router. It's a no-op
# when `INGEST_API_KEY` is unset (local dev), and rejects with 401 otherwise.
ingest_router = APIRouter(prefix="/ingest", tags=["ingest"], dependencies=[Depends(require_api_key)])
incidents_router = APIRouter(prefix="/incidents", tags=["incidents"])
signals_router = APIRouter(prefix="/signals", tags=["signals"])
notifications_router = APIRouter(prefix="/notifications", tags=["notifications"])
ws_router = APIRouter()


def _client_key(request: Request) -> str:
    return request.client.host if request.client else "unknown"


def _enqueue(request: Request, signal: Signal) -> None:
    """Apply rate-limit + push onto the bounded queue. The two ways to be
    rejected (rate-limited / queue-full) both surface as 429 so producers
    have a single retry rule."""
    state = request.app.state
    if not state.rate_limiter.allow(_client_key(request)):
        metrics.incr("rate_limited")
        raise HTTPException(status_code=429, detail="rate limit exceeded")
    try:
        state.queue.offer(signal)
    except QueueFull as exc:
        metrics.incr("signals_dropped_backpressure")
        raise HTTPException(
            status_code=429,
            detail="queue full — apply backpressure and retry",
            headers={"Retry-After": "1"},
        ) from exc
    metrics.incr("signals_ingested")
    metrics.gauge("queue_depth", state.queue.qsize())


# ---- ops endpoints ---------------------------------------------------------
def _wants_html(request: Request) -> bool:
    """Browsers send `Accept: text/html,...`; curl / Prometheus do not."""
    accept = request.headers.get("accept", "")
    return "text/html" in accept and "application/json" not in accept.split(",")[0]


@ops_router.get("/health")
async def health(request: Request):
    """Liveness + readiness probe.

    Content-negotiation:
      * Browser (Accept: text/html) → styled HTML page with per-dep traffic-lights
      * curl / Kubernetes / anything else → JSON  (the canonical format)
    """
    state = request.app.state
    deps: dict[str, str] = {}
    try:
        async with state.postgres.pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        deps["postgres"] = "ok"
    except Exception as exc:
        deps["postgres"] = f"down: {exc}"
    try:
        await state.mongo.client.admin.command("ping")
        deps["mongo"] = "ok"
    except Exception as exc:
        deps["mongo"] = f"down: {exc}"
    try:
        deps["redis"] = "ok" if await state.cache.client.ping() else "down"
    except Exception as exc:
        deps["redis"] = f"down: {exc}"

    body = {
        "status": "ok" if all(v == "ok" for v in deps.values()) else "degraded",
        "dependencies": deps,
        "queue_depth": state.queue.qsize(),
        "queue_capacity": state.queue.maxsize,
    }
    if _wants_html(request):
        return HTMLResponse(_render_health_html(body))
    return JSONResponse(body)


@ops_router.get("/metrics")
async def prometheus_metrics(request: Request):
    """Prometheus exposition.

    Content-negotiation:
      * Browser → styled HTML dashboard of metric tiles (auto-refresh 3 s)
      * Anything else (curl, prometheus_scraper) → text/plain Prometheus format
    """
    if _wants_html(request):
        return HTMLResponse(_render_metrics_html(metrics.prometheus()))
    return Response(content=metrics.prometheus(), media_type="text/plain; version=0.0.4")


# ---- HTML renderers (browser-friendly views of /health and /metrics) -------
def _render_health_html(data: dict) -> str:
    overall = data["status"]
    overall_class = "ok" if overall == "ok" else "degraded"
    rows = "".join(
        f'<tr><td>{name}</td><td class="{("ok" if v == "ok" else "down")}">{v}</td></tr>'
        for name, v in data["dependencies"].items()
    )
    return f"""<!doctype html>
<html><head><meta charset="utf-8"><title>IMS · /health</title>
<meta http-equiv="refresh" content="5">
<style>
  body {{ font-family: ui-sans-serif, system-ui, -apple-system, sans-serif;
         background: #0b1020; color: #e2e8f0; margin: 0; padding: 32px; }}
  h1 {{ margin: 0 0 4px; font-size: 22px; }}
  .sub {{ color: #94a3b8; font-size: 12px; margin-bottom: 24px; }}
  .pill {{ display: inline-block; padding: 4px 12px; border-radius: 999px;
          font-weight: 600; font-size: 13px; margin-left: 8px; }}
  .pill.ok       {{ background: #064e3b; color: #6ee7b7; }}
  .pill.degraded {{ background: #7c2d12; color: #fdba74; }}
  table {{ border-collapse: collapse; min-width: 320px;
          background: #111827; border: 1px solid #1f2937; border-radius: 8px;
          overflow: hidden; margin-bottom: 16px; }}
  td {{ padding: 8px 14px; font-size: 14px; }}
  tr {{ border-bottom: 1px solid #1f2937; }} tr:last-child {{ border-bottom: none; }}
  td.ok   {{ color: #6ee7b7; }}
  td.down {{ color: #fca5a5; }}
  .kv {{ display: grid; grid-template-columns: max-content auto; gap: 4px 16px;
        font-size: 13px; color: #94a3b8; }}
  .kv b {{ color: #cbd5e1; font-weight: 600; }}
  a {{ color: #818cf8; text-decoration: none; }} a:hover {{ text-decoration: underline; }}
</style></head>
<body>
  <h1>IMS · health <span class="pill {overall_class}">{overall}</span></h1>
  <div class="sub">auto-refresh every 5 s · also available as JSON via <code>curl /health</code></div>
  <table>{rows}</table>
  <div class="kv">
    <b>queue depth</b><span>{data['queue_depth']} / {data['queue_capacity']}</span>
  </div>
  <p style="margin-top:32px;font-size:12px;color:#64748b">
    <a href="/metrics">/metrics</a> · <a href="/docs">/docs</a> · <a href="http://localhost:5173">/dashboard →</a>
  </p>
</body></html>"""


def _render_metrics_html(prom_text: str) -> str:
    """Parse a tiny subset of the Prometheus exposition into nice tiles."""
    tiles = []
    for line in prom_text.strip().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        try:
            name, value = line.split(maxsplit=1)
        except ValueError:
            continue
        nice = name.replace("ims_", "").replace("_", " ")
        tiles.append((nice, value))
    cards = "".join(
        f'<div class="card"><div class="lbl">{n}</div><div class="val">{v}</div></div>'
        for n, v in tiles
    )
    return f"""<!doctype html>
<html><head><meta charset="utf-8"><title>IMS · /metrics</title>
<meta http-equiv="refresh" content="3">
<style>
  body {{ font-family: ui-sans-serif, system-ui, sans-serif;
         background: #0b1020; color: #e2e8f0; margin: 0; padding: 32px; }}
  h1 {{ margin: 0 0 4px; font-size: 22px; }}
  .sub {{ color: #94a3b8; font-size: 12px; margin-bottom: 24px; }}
  .grid {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(220px, 1fr));
          gap: 12px; margin-bottom: 24px; }}
  .card {{ background: #111827; border: 1px solid #1f2937; border-radius: 10px;
          padding: 14px 16px; }}
  .lbl {{ font-size: 11px; text-transform: uppercase; letter-spacing: .04em;
         color: #94a3b8; }}
  .val {{ font-size: 22px; font-weight: 600; color: #a5b4fc; margin-top: 6px;
         font-variant-numeric: tabular-nums; }}
  pre {{ background: #0f172a; border: 1px solid #1f2937; border-radius: 8px;
        padding: 12px 16px; font-size: 12px; color: #cbd5e1;
        overflow-x: auto; white-space: pre-wrap; }}
  a {{ color: #818cf8; text-decoration: none; }} a:hover {{ text-decoration: underline; }}
</style></head>
<body>
  <h1>IMS · metrics</h1>
  <div class="sub">auto-refresh every 3 s · raw Prometheus format below · also via <code>curl /metrics</code></div>
  <div class="grid">{cards}</div>
  <pre>{prom_text}</pre>
  <p style="font-size:12px;color:#64748b">
    <a href="/health">/health</a> · <a href="/docs">/docs</a> · <a href="http://localhost:5173">/dashboard →</a>
  </p>
</body></html>"""


# ---- ingest endpoints ------------------------------------------------------
@ingest_router.post("", status_code=202)
async def ingest_one(payload: SignalIn, request: Request) -> dict:
    signal = Signal.from_input(payload)
    _enqueue(request, signal)
    return {"accepted": True, "signal_id": signal.signal_id}


class _BatchIn(BaseModel):
    signals: List[SignalIn]


@ingest_router.post("/batch", status_code=202)
async def ingest_batch(payload: _BatchIn, request: Request, response: Response) -> dict:
    """Batch endpoint — much more efficient at high rate (one HTTP RTT for N
    signals). On partial backpressure we still accept what we can."""
    state = request.app.state
    if not state.rate_limiter.allow(_client_key(request), cost=len(payload.signals)):
        metrics.incr("rate_limited")
        raise HTTPException(status_code=429, detail="rate limit exceeded")
    accepted: list[str] = []
    rejected = 0
    for s in payload.signals:
        signal = Signal.from_input(s)
        try:
            state.queue.offer(signal)
            accepted.append(signal.signal_id)
            metrics.incr("signals_ingested")
        except QueueFull:
            rejected += 1
            metrics.incr("signals_dropped_backpressure")
    metrics.gauge("queue_depth", state.queue.qsize())
    if rejected and not accepted:
        response.status_code = 429
    return {"accepted": len(accepted), "rejected": rejected, "ids": accepted}


@ingest_router.post("/simulate", status_code=202)
async def ingest_simulate(
    request: Request,
    rate: int = Query(1000, ge=1, le=20_000, description="signals per second"),
    duration: int = Query(5, ge=1, le=60, description="seconds"),
) -> dict:
    """Bonus: in-process load generator — fires N sig/s for `duration` s.

    Runs as a background task so the call returns immediately. Drives the
    "Simulate signal storm" button in the dashboard.
    """
    components = [
        ("rdbms-primary", ComponentType.RDBMS),
        ("api-gateway", ComponentType.API),
        ("cache-cluster-01", ComponentType.CACHE),
        ("mcp-host-eu", ComponentType.MCP_HOST),
        ("queue-events", ComponentType.QUEUE),
        ("nosql-mongo", ComponentType.NOSQL),
    ]
    queue = request.app.state.queue

    async def runner() -> None:
        end = asyncio.get_event_loop().time() + duration
        per_tick = max(rate // 10, 1)
        while asyncio.get_event_loop().time() < end:
            for _ in range(per_tick):
                comp_id, comp_type = random.choice(components)
                signal = Signal.from_input(
                    SignalIn(
                        component_id=comp_id,
                        component_type=comp_type,
                        message=f"simulated failure on {comp_id}",
                        latency_ms=random.uniform(50, 4500),
                        error_code=random.choice(["E_TIMEOUT", "E_CONN", "E_5XX"]),
                    )
                )
                try:
                    queue.offer(signal)
                    metrics.incr("signals_ingested")
                except QueueFull:
                    metrics.incr("signals_dropped_backpressure")
            await asyncio.sleep(0.1)

    asyncio.create_task(runner())
    return {"started": True, "rate_per_second": rate, "duration_seconds": duration}


# ---- incidents endpoints ---------------------------------------------------
class _TransitionRequest(BaseModel):
    target_status: WorkItemStatus


@incidents_router.get("")
async def list_incidents(
    request: Request,
    status: Optional[WorkItemStatus] = None,
    limit: int = Query(100, ge=1, le=500),
    use_cache: bool = Query(True, description="serve hot dashboard from Redis"),
) -> list[dict]:
    state = request.app.state
    # Hot path: the dashboard hits this every few seconds; serving from Redis
    # avoids hammering Postgres.
    if use_cache and status is None:
        cached = await state.cache.list_dashboard()
        if cached:
            cached.sort(key=lambda x: (x.get("severity", "P3"), -_unix(x.get("created_at"))))
            return cached[:limit]
    items = await state.work_item_repo.list(status=status.value if status else None, limit=limit)
    return [w.model_dump(mode="json") for w in items]


@incidents_router.get("/stats")
async def stats(request: Request) -> dict:
    return await request.app.state.work_item_repo.stats()


@incidents_router.get("/timeseries/{kind}")
async def time_series(
    kind: str, request: Request, since_seconds: int = Query(3600, ge=60, le=86400)
) -> list[dict]:
    if kind not in ("signals", "incidents"):
        raise HTTPException(status_code=400, detail="kind must be signals|incidents")
    return await request.app.state.cache.time_series(kind, since_seconds)


@incidents_router.get("/{work_item_id}")
async def get_incident(work_item_id: str, request: Request) -> dict:
    state = request.app.state
    wi = await state.work_item_repo.get(work_item_id)
    if not wi:
        raise HTTPException(status_code=404, detail="work item not found")
    rca = await state.rca_repo.get(work_item_id)
    signals = await state.signal_repo.list_for_work_item(work_item_id, limit=200)
    return {
        "work_item": wi.model_dump(mode="json"),
        "rca": rca.model_dump(mode="json") if rca else None,
        "signals": signals,
    }


@incidents_router.post("/{work_item_id}/transition")
async def transition_incident(
    work_item_id: str, body: _TransitionRequest, request: Request
) -> dict:
    engine: WorkflowEngine = request.app.state.engine
    try:
        wi = await engine.transition(work_item_id, body.target_status)
    except IllegalTransition as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    except RCAValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return wi.model_dump(mode="json")


@incidents_router.put("/{work_item_id}/rca")
async def submit_rca(work_item_id: str, body: RCA, request: Request) -> dict:
    if body.work_item_id != work_item_id:
        raise HTTPException(status_code=400, detail="work_item_id mismatch")
    try:
        rca = await request.app.state.engine.submit_rca(body)
    except RCAValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    return rca.model_dump(mode="json")


# ---- signals (data-lake search) -------------------------------------------
@signals_router.get("/search")
async def search_signals(
    request: Request,
    component_id: Optional[str] = None,
    component_type: Optional[str] = None,
    error_code: Optional[str] = None,
    text: Optional[str] = Query(None, description="case-insensitive substring on message"),
    limit: int = Query(100, ge=1, le=500),
) -> list[dict]:
    """Demonstrate the queryability of the raw-payload data lake."""
    return await request.app.state.signal_repo.search(
        component_id=component_id,
        component_type=component_type,
        error_code=error_code,
        text=text,
        limit=limit,
    )


# ---- notifications (the bell icon in the dashboard) -----------------------
@notifications_router.get("")
async def list_notifications(
    request: Request,
    limit: int = Query(50, ge=1, le=200),
    unread_only: bool = Query(False, description="if true, only unread items"),
) -> list[dict]:
    """Returns the most-recent notifications (newest first).

    The bell icon polls this every few seconds; the WebSocket also pushes
    new entries instantly so the badge updates without a full refresh.
    """
    store = request.app.state.notification_store
    return await store.list(limit=limit, unread_only=unread_only)


@notifications_router.get("/unread_count")
async def unread_count(request: Request) -> dict:
    return {"unread": await request.app.state.notification_store.unread_count()}


@notifications_router.post("/{notification_id}/ack")
async def ack_notification(notification_id: str, request: Request) -> dict:
    ok = await request.app.state.notification_store.ack(notification_id)
    if not ok:
        raise HTTPException(status_code=404, detail="notification not found")
    return {"acked": notification_id}


@notifications_router.post("/ack_all")
async def ack_all(request: Request) -> dict:
    n = await request.app.state.notification_store.ack_all()
    return {"acked": n}


@notifications_router.delete("")
async def clear_notifications(request: Request) -> dict:
    await request.app.state.notification_store.clear()
    return {"cleared": True}


# ---- WebSocket -------------------------------------------------------------
@ws_router.websocket("/ws")
async def ws_endpoint(ws: WebSocket) -> None:
    await broadcaster.add(ws)
    try:
        while True:
            # We don't expect inbound traffic — receive_text just keeps the
            # socket alive and detects client disconnects.
            await ws.receive_text()
    except (WebSocketDisconnect, Exception):
        await broadcaster.remove(ws)


# ---- helpers ---------------------------------------------------------------
def _unix(value) -> float:
    """Tolerant ISO-string → epoch conversion (returns 0 on anything weird)."""
    if not value:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    try:
        from datetime import datetime
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
    except Exception:
        return 0.0
