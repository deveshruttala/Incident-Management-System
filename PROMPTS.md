# Prompts & Design Strategy

This document captures the **design strategy** that shaped the Incident
Management System and the **prompts used during development** with an LLM
coding assistant. 

For the running architecture diagram, layer-by-layer breakdown, and class
layout, see [`ARCHITECTURE.md`](./ARCHITECTURE.md). For the polished
submission write-up, see the project `.docx`.

---

## 1. Design strategy

The design follows five guiding principles. Every concrete choice traces
back to one of them.

### 1.1 Decouple ingest from storage
The producer must never wait for the database. A bounded `asyncio.Queue`
(`maxsize=50_000`) sits between the HTTP edge and the workers; full → HTTP 429
+ `Retry-After: 1`. Memory is capped (~25 MB worst case) so the process
cannot OOM regardless of how slow the storage layer becomes.

### 1.2 Right tool for each storage purpose
Three tiers, each picked for one property the others can't match:

- **MongoDB** — schemaless audit log; indexed on `(component_id, received_at)`,
  `work_item_id`, `received_at`. Queryable via `GET /signals/search`.
- **PostgreSQL** — ACID transactions; FK from `rcas.work_item_id →
  work_items` enforces RCA-before-CLOSE *at the database level*. App code
  cannot bypass it.
- **Redis** — sub-ms HASH for the dashboard hot-path; `HINCRBY` per-minute
  buckets for time-series. One container, two roles.

### 1.3 Patterns that make extensibility a one-class change
- **State Pattern** for the lifecycle. Each state owns its allowed transitions
  and preconditions. The RCA gate lives inside `ResolvedState.validate_transition()`
  — every code path that closes a Work Item gets the check for free.
- **Strategy Pattern** for alert routing. `select_strategy(component_type)`
  returns one of P0..P3 strategies. Adding a severity is one new class plus
  one map entry.
- **Sink Protocol** behind every strategy. The in-memory `NotificationStore`
  proves substitutability today; a real PagerDuty client tomorrow is one
  new class with a single `push()` method.
- **Repository Pattern** over every store. Business logic depends on the
  abstraction, not the driver — swap MongoDB for DynamoDB by replacing one
  class.

### 1.4 Race-free debouncing
Naive deduplication produced 41 Work Items for one component under burst
load — two workers can elect themselves leader at the same microsecond.
Fix: a **per-component `asyncio.Lock`** (not a global lock) so different
components stay parallel while the same component serialises. Verified
fix: 305 raw signals → 6 Work Items.

### 1.5 Defence in depth
A single producer contract — *"if you see HTTP 429, back off and retry"* —
is backed by **five layers**: bounded queue, per-IP token-bucket rate
limiter, async-everywhere worker pool, Tenacity retries on every storage
write (3 attempts, exp-backoff 100 ms → 2 s), and per-storage failure
isolation so no single outage takes down the pipeline. Verified live:
10 000 sig/s × 5 s burst → 46 305 accepted, 0 dropped.

---

## 2. Prompts used during development

The repo was bootstrapped and iterated with the help of an LLM coding
assistant. All notable prompts (paraphrased), grouped by phase.

### Phase A — Architecture & scaffolding

1. *"Design a Python FastAPI ingestion pipeline that handles 10 k req/s using
   asyncio with bounded-queue backpressure. Show the queue/worker structure,
   the HTTP-handler contract, and exactly how the system avoids OOM when the
   downstream database is slow."*
2. *"Sketch a Docker Compose with FastAPI, Postgres, MongoDB, Redis, and a
   Vite-built React frontend behind nginx. Use multi-stage Dockerfiles, a
   non-root user (`uid=999`), and a healthcheck for every service so
   `docker compose up` reports `(healthy)` for all five containers."*
3. *"Recommend the right tool for each storage role: raw payloads,
   transactional Work Items, dashboard cache, time-series. For each one,
   justify the choice and call out the alternative I'm rejecting."*
4. *"Lay out the FastAPI backend in 8 files — `main.py`, `config.py`,
   `models.py`, `core.py`, `storage.py`, `workflow.py`, `notifications.py`,
   `routes.py`, `security.py` — with one concern per file and a strict
   import DAG (no cycles)."*

### Phase B — Patterns & data flow

5. *"Implement the GoF State Pattern in Python for an incident workflow with
   states OPEN → INVESTIGATING → RESOLVED → CLOSED. Each state class owns
   its `allowed_transitions()` and a `validate_transition()` precondition
   check. Make CLOSED reject when the RCA is missing or incomplete; raise a
   typed `RCAValidationError` that maps cleanly to HTTP 422."*
6. *"Add a CLOSED → OPEN reopen edge that clears `end_time` and `mttr` but
   preserves the original RCA. Re-CLOSE should recompute MTTR against the
   *original* `start_time`, not the reopen time."*
7. *"Generate a Strategy pattern for alerting with severities P0..P3 mapped
   from component types: RDBMS / MCP_HOST → P0 (PagerDuty), API / QUEUE →
   P1 (on-call), CACHE / NOSQL → P2 (Slack), default → P3 (email digest).
   Make selection a single function, not an if/elif/else cascade."*
8. *"Define a `NotificationSink` Protocol with one method `push(notification)`.
   Refactor every `AlertStrategy.fire()` to write to this Protocol so the
   strategy doesn't know whether the sink is in-memory, PagerDuty, or
   anything else. Show that adding a new sink is a one-class change."*
9. *"Implement an in-memory `NotificationStore` against the `NotificationSink`
   Protocol — bounded `deque(maxlen=200)`, integrates with the WebSocket
   broadcaster so the dashboard sees alerts in real time."*
10. *"Write a debouncer keyed by `component_id` with a 10-second window.
    First signal opens a Work Item; subsequent signals within the window
    link to it in MongoDB. Use a **per-component** `asyncio.Lock` (not
    global) so different components still parallelise while same-component
    bursts serialise into one Work Item."*
11. *"The naive debouncer produces multiple Work Items for the same
    component under burst load because two workers race to be the leader.
    Walk me through the exact race condition and fix it inside the lock
    boundary — show the before/after code."*
12. *"Build the Repository Pattern over Mongo, Postgres, and Redis:
    `SignalRepository`, `WorkItemRepository`, `RCARepository`. Business
    logic depends on the repo interface only; the driver lives behind it
    so swapping MongoDB for DynamoDB tomorrow is a one-class change."*
13. *"Wrap every storage write in a `with_retry()` helper using Tenacity:
    3 attempts, exponential backoff 100 ms → 200 ms → 400 ms, capped at
    2 s. Use a factory-callback pattern because each retry needs a *fresh*
    awaitable — coroutines can only be awaited once."*

### Phase C — Frontend (UI / UX)

14. *"Create a React 18 + Vite + Tailwind incident dashboard with a live
    WebSocket feed (auto-reconnect every 2 s), an incident detail drawer,
    and an RCA form with two `<input type=\"datetime-local\">`, a
    `<select>` for root-cause category, and two `<textarea>` with live
    char-count hints (amber if too short)."*
15. *"Add a NotificationBell in the header — unread count badge + dropdown
    of recent alerts. Use the existing WebSocket connection; subscribe to
    `notification` events; ack on click; show the icon in red if unread > 0."*
16. *"Sort the live feed by severity (P0 first), with ties broken by
    recency. Add a free-text search bar above the feed that filters by
    incident title, component type, or component id."*
17. *"Make the layout responsive: single pane with a `← Back to feed`
    button on mobile (< 768 px); side-by-side feed + detail on tablet and
    larger. Use Tailwind breakpoints; keep the bell icon visible at every
    width."*
18. *"Parse FastAPI / Pydantic 422 validation errors into a structured
    `ApiError` class. Render them in a friendly `ErrorBanner` component
    with field-level lists, status-coded colours, and no raw JSON
    surfacing in the UI."*
19. *"Eliminate every explicit `any` type from the frontend. Replace
    `catch (e: any)` with `unknown` + a `toError()` helper; type the
    WebSocket payloads with a discriminated union (`work_item_created |
    work_item_updated | notification`)."*
20. *"Add an in-app load-generator button (🚨 in the header) that POSTs
    `/ingest/simulate?rate=2000&seconds=10` so demos can fire a 10 k-burst
    storm without leaving the dashboard."*

### Phase D — Resilience, observability, security

21. *"Expose a `/health` endpoint that probes Postgres, MongoDB, and Redis
    in parallel. Serve **content-negotiated** output: JSON to `curl` /
    tools (Accept: application/json), styled HTML to browsers (auto-refresh
    every 5 s) so an operator can hit the URL with either."*
22. *"Add a Prometheus-style `/metrics` endpoint with counters
    (`ims_signals_ingested`, `ims_signals_processed`, `ims_signals_debounced`,
    `ims_signals_dropped_backpressure`, `ims_rate_limited`,
    `ims_incidents_created`) and gauges (`ims_queue_depth`)."*
23. *"Print throughput to the console every 5 seconds in a single line:
    `[throughput] ingest=N/s process=N/s drop=N/s queue=N total_in=N total_out=N`.
    Background asyncio task started in the FastAPI lifespan."*
24. *"Implement a per-IP token-bucket rate limiter where the cost of each
    request equals `len(batch)` so a 1 000-signal batch consumes 1 000
    tokens — batching cannot bypass the limiter. Default 12 k req/s
    refill, 20 k burst."*
25. *"Add an optional API-key check on `/ingest` using
    `secrets.compare_digest` (timing-attack safe). Skip the check entirely
    if the `INGEST_API_KEY` env var is unset, so dev mode stays
    frictionless."*
26. *"Add a `BodySizeLimitMiddleware` that rejects requests with
    `Content-Length` > 1 MiB → HTTP 413, before the handler runs. Defence
    against memory abuse via huge requests."*
27. *"Add `X-Request-ID` correlation IDs — auto-generate a UUID if not
    provided, propagate through every log line, echo on every response.
    Implement as ASGI middleware so every endpoint gets it for free."*
28. *"Implement per-storage failure isolation: if Mongo write fails after
    retries, log + drop and keep ingesting (Work Item still goes to
    Postgres); if Redis cache write fails, dashboard misses one update but
    pipeline continues; if Postgres write fails, log and let next signal
    retry the create."*

### Phase E — Testing & validation

29. *"Write pytest-asyncio tests for: the RCA gate on `RESOLVED → CLOSED`
    (HTTP 422 if RCA incomplete), MTTR calculation on close, MTTR-clear on
    reopen, debouncer race-condition (100 concurrent signals for one
    component → exactly 1 Work Item), `NotificationStore` push/ack/bounded
    buffer, rate-limiter token consumption, and per-storage retry failure
    isolation."*
30. *"Write a CLI `scripts/simulate_failure.py` that reproduces the spec
    example: RDBMS outage cascading into MCP failure with realistic signal
    patterns over ~30 seconds. Print before/after counts so a reviewer can
    verify the debouncer collapsed N raw signals into M Work Items."*
31. *"Silence the `pytest-asyncio` deprecation warning by setting
    `asyncio_default_fixture_loop_scope = function` in `pytest.ini`."*

### Phase F — Packaging, CI, documentation

32. *"Write a GitHub Actions workflow that runs `pytest` + builds both
    Docker images on every push to `main` and every pull request. Cache
    pip and Docker layers so the green tick lands in under 2 minutes."*
33. *"Generate a project README with: 3-paragraph summary, embedded
    architecture diagram (centred, max-width 600 px), Docker Compose
    setup, shortened backpressure section, and links to ARCHITECTURE.md
    and PROMPTS.md. Treat it as the reviewer's *first* impression."*
34. *"Generate a Word document of the submission as a technical and
    non-technical abstract — 8 pages max, embedded architecture image on
    page 2, every bullet ≤ 25 words (split if longer), with a 3-point
    future-scope section. Tighter typography (10pt body, 16pt H1, 12pt H2)."*
35. *"Write `ARCHITECTURE.md` with two Mermaid class diagrams: State
    Pattern + Strategy/Sink Protocol. Use attribute lines (`next :
    INVESTIGATING, RESOLVED`) instead of literal `{}` braces inside class
    bodies — Mermaid's classDiagram parser treats `{` as a nested-struct
    opener and crashes."*

---

*— end of document —*
