// Single source of truth for the frontend ↔ backend contract.
// Contains the TypeScript domain types, a tiny REST client, and the
// WebSocket hook used by the dashboard for live updates.

import { useEffect, useRef } from "react";

// --------------------------------------------------------------------------
// Domain types — mirror backend Pydantic models
// --------------------------------------------------------------------------
export type Severity = "P0" | "P1" | "P2" | "P3";
export type Status = "OPEN" | "INVESTIGATING" | "RESOLVED" | "CLOSED";

export type RootCauseCategory =
  | "INFRASTRUCTURE"
  | "DEPLOYMENT"
  | "CONFIG_CHANGE"
  | "DEPENDENCY_FAILURE"
  | "CODE_DEFECT"
  | "CAPACITY"
  | "HUMAN_ERROR"
  | "UNKNOWN";

export interface WorkItem {
  work_item_id: string;
  component_id: string;
  component_type: string;
  severity: Severity;
  status: Status;
  title: string;
  signal_count: number;
  start_time: string;
  end_time: string | null;
  mttr_seconds: number | null;
  created_at: string;
  updated_at: string;
}

export interface RCA {
  work_item_id: string;
  incident_start: string;
  incident_end: string;
  root_cause_category: RootCauseCategory;
  fix_applied: string;
  prevention_steps: string;
  submitted_by?: string;
  submitted_at?: string;
}

// Raw signal payload as it sits in MongoDB. All fields optional because
// historical / external producers may omit some — the renderer must cope.
export interface RawSignal {
  _id?: string;
  signal_id?: string;
  component_id?: string;
  component_type?: string;
  message?: string;
  error_code?: string | null;
  latency_ms?: number | null;
  received_at?: string;
  occurred_at?: string;
  work_item_id?: string;
  payload?: Record<string, unknown>;
}

export interface IncidentDetail {
  work_item: WorkItem;
  rca: RCA | null;
  signals: RawSignal[];
}

export interface Stats {
  by_status: Record<string, number>;
  avg_mttr_seconds: number;
  closed_count: number;
}

export interface Health {
  status: string;
  dependencies: Record<string, string>;
  queue_depth: number;
  queue_capacity: number;
}

export interface TimeBucket {
  bucket_ts: number;
  count: number;
}

// One row in the notification bell dropdown. Mirrors NotificationDict
// in backend/app/notifications.py — keep field names in sync.
export interface Notification {
  id: string;
  severity: Severity;
  component_type: string;
  component_id: string;
  title: string;
  channel: string;        // "PagerDuty (on-call)" | "Slack #ops" | …
  work_item_id: string;
  timestamp: number;      // unix epoch seconds
  read: boolean;
}

// --------------------------------------------------------------------------
// Structured error — what every api.* call throws on a non-2xx response.
// We parse FastAPI / Pydantic responses so the UI can render friendly per-
// field messages instead of dumping raw JSON.
// --------------------------------------------------------------------------
export interface FieldIssue {
  field: string;          // e.g. "fix_applied"
  message: string;        // e.g. "should have at least 10 characters"
}

// Shape of a single Pydantic validation issue inside FastAPI's `detail` array.
interface PydanticIssue {
  loc: unknown[];
  msg: string;
  type?: string;
  ctx?: Record<string, unknown>;
}

// FastAPI's error response always wraps the payload in `{ detail: ... }`.
// `detail` is either a string (HTTPException) or an array of PydanticIssue.
interface FastApiErrorBody {
  detail?: string | PydanticIssue[];
}

export class ApiError extends Error {
  status: number;
  title: string;          // short headline (e.g. "Validation failed")
  detail: string;         // human-readable summary line
  issues: FieldIssue[];   // populated for Pydantic validation errors
  raw: unknown;           // original response body for debugging

  constructor(status: number, title: string, detail: string, issues: FieldIssue[], raw: unknown) {
    super(detail);
    this.name = "ApiError";
    this.status = status;
    this.title = title;
    this.detail = detail;
    this.issues = issues;
    this.raw = raw;
  }
}

const PRETTY_FIELD: Record<string, string> = {
  fix_applied: "Fix applied",
  prevention_steps: "Prevention steps",
  root_cause_category: "Root cause category",
  incident_start: "Incident start",
  incident_end: "Incident end",
  submitted_by: "Submitted by",
  target_status: "Target status",
};

function prettyField(loc: unknown[]): string {
  // FastAPI puts loc as ["body", "fix_applied"] or ["body", "signals", 2, "component_id"]
  const parts = loc.filter((p) => p !== "body");
  const last = String(parts[parts.length - 1] ?? "");
  return PRETTY_FIELD[last] ?? last.replace(/_/g, " ");
}

function prettyMessage(msg: string): string {
  // Pydantic messages start with "String " etc; trim the noise.
  return msg.replace(/^String /i, "").replace(/^Value /i, "");
}

async function parseError(res: Response): Promise<ApiError> {
  let body: FastApiErrorBody | null = null;
  try {
    body = (await res.json()) as FastApiErrorBody;
  } catch {
    return new ApiError(res.status, `${res.status} ${res.statusText}`, await res.text() || res.statusText, [], null);
  }

  const detail = body?.detail;

  // Case A — Pydantic validation: detail is an array of issues
  if (Array.isArray(detail)) {
    const issues: FieldIssue[] = detail.map((d: PydanticIssue) => ({
      field: prettyField(d.loc ?? []),
      message: prettyMessage(d.msg ?? "is invalid"),
    }));
    const title = res.status === 422 ? "Validation failed" : `${res.status} ${res.statusText}`;
    const summary =
      issues.length === 1
        ? `${issues[0].field}: ${issues[0].message}`
        : `${issues.length} fields need attention`;
    return new ApiError(res.status, title, summary, issues, body);
  }

  // Case B — single string detail (our HTTPException raises)
  if (typeof detail === "string") {
    let title = `${res.status} ${res.statusText}`;
    if (res.status === 422) title = "Validation failed";
    else if (res.status === 409) title = "Illegal transition";
    else if (res.status === 401) title = "Unauthorised";
    else if (res.status === 429) title = "Rate limited";
    else if (res.status === 404) title = "Not found";
    return new ApiError(res.status, title, detail, [], body);
  }

  // Case C — anything else
  return new ApiError(res.status, `${res.status} ${res.statusText}`, JSON.stringify(body), [], body);
}

// --------------------------------------------------------------------------
// REST client — `BASE` is /api so nginx (or Vite) reverse-proxies to backend
// --------------------------------------------------------------------------
const BASE = "/api";

async function http<T>(path: string, opts: RequestInit = {}): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    headers: { "Content-Type": "application/json" },
    ...opts,
  });
  if (!res.ok) throw await parseError(res);
  return (await res.json()) as T;
}

export const api = {
  listIncidents: (status?: Status) =>
    http<WorkItem[]>(`/incidents${status ? `?status=${status}` : ""}`),
  getIncident: (id: string) => http<IncidentDetail>(`/incidents/${id}`),
  stats: () => http<Stats>(`/incidents/stats`),
  transition: (id: string, target_status: Status) =>
    http<WorkItem>(`/incidents/${id}/transition`, {
      method: "POST",
      body: JSON.stringify({ target_status }),
    }),
  submitRCA: (id: string, rca: RCA) =>
    http<RCA>(`/incidents/${id}/rca`, { method: "PUT", body: JSON.stringify(rca) }),
  simulate: (rate = 2000, duration = 10) =>
    http<{ started: boolean }>(`/ingest/simulate?rate=${rate}&duration=${duration}`, {
      method: "POST",
    }),
  health: () => http<Health>("/health"),
  timeseries: (kind: "signals" | "incidents", since_seconds = 3600) =>
    http<TimeBucket[]>(`/incidents/timeseries/${kind}?since_seconds=${since_seconds}`),

  // Notification bell — small in-process store on the backend, wired via
  // the Strategy Pattern (workflow.py) → NotificationSink (notifications.py).
  notifications: {
    list: (limit = 50, unread_only = false) =>
      http<Notification[]>(
        `/notifications?limit=${limit}&unread_only=${unread_only}`,
      ),
    unreadCount: () => http<{ unread: number }>("/notifications/unread_count"),
    ack: (id: string) =>
      http<{ acked: string }>(`/notifications/${id}/ack`, { method: "POST" }),
    ackAll: () => http<{ acked: number }>("/notifications/ack_all", { method: "POST" }),
    clear: () => http<{ cleared: boolean }>("/notifications", { method: "DELETE" }),
  },
};

// --------------------------------------------------------------------------
// WebSocket — typed messages + auto-reconnecting hook.
// The backend emits three event types over the same /ws connection:
//   - work_item_created / work_item_updated  (data: WorkItem)
//   - notification                           (data: Notification)
// A discriminated union lets consumers narrow with a single switch.
// --------------------------------------------------------------------------
export type WSMessage =
  | { event: "work_item_created" | "work_item_updated"; data: WorkItem }
  | { event: "notification"; data: Notification };

export function useIncidentWebSocket(onMessage: (msg: WSMessage) => void) {
  // Use a ref so the latest callback is always invoked without re-subscribing.
  const handlerRef = useRef(onMessage);
  handlerRef.current = onMessage;

  useEffect(() => {
    const proto = window.location.protocol === "https:" ? "wss:" : "ws:";
    const url = `${proto}//${window.location.host}/ws`;
    let ws: WebSocket | null = null;
    let reconnectTimer: number | null = null;

    const connect = () => {
      ws = new WebSocket(url);
      ws.onmessage = (evt) => {
        try {
          const parsed = JSON.parse(evt.data) as WSMessage;
          if (parsed && typeof parsed.event === "string" && parsed.data) {
            handlerRef.current(parsed);
          }
        } catch {
          /* ignore malformed frames */
        }
      };
      ws.onclose = () => {
        reconnectTimer = window.setTimeout(connect, 2000);
      };
      ws.onerror = () => ws?.close();
    };

    connect();
    return () => {
      if (reconnectTimer) window.clearTimeout(reconnectTimer);
      ws?.close();
    };
  }, []);
}
