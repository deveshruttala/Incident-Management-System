// Top-level dashboard: header, metrics bar, live incident feed and the
// detail pane. Small presentational pieces (badges, the metrics tiles) live
// here too — there's no value in splitting them into separate files.

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { api, useIncidentWebSocket } from "./api";
import type {
  Health,
  Notification,
  Severity,
  Status,
  Stats,
  TimeBucket,
  WorkItem,
} from "./api";
import { IncidentDetailView } from "./IncidentDetail";

const SEVERITY_RANK = { P0: 0, P1: 1, P2: 2, P3: 3 } as const;

const SEVERITY_STYLE: Record<Severity, string> = {
  P0: "bg-red-500/20 text-red-300 border-red-500/40",
  P1: "bg-orange-500/20 text-orange-300 border-orange-500/40",
  P2: "bg-yellow-500/20 text-yellow-300 border-yellow-500/40",
  P3: "bg-sky-500/20 text-sky-300 border-sky-500/40",
};

const STATUS_STYLE: Record<Status, string> = {
  OPEN: "bg-red-500/20 text-red-200 border-red-500/40",
  INVESTIGATING: "bg-amber-500/20 text-amber-200 border-amber-500/40",
  RESOLVED: "bg-emerald-500/20 text-emerald-200 border-emerald-500/40",
  CLOSED: "bg-slate-500/20 text-slate-300 border-slate-500/40",
};

// --------------------------------------------------------------------------
// Reusable badges (also used by IncidentDetail.tsx via re-export below)
// --------------------------------------------------------------------------
export function SeverityBadge({ severity }: { severity: Severity }) {
  return (
    <span className={`inline-block rounded-md border px-2 py-0.5 text-xs font-semibold ${SEVERITY_STYLE[severity]}`}>
      {severity}
    </span>
  );
}

export function StatusBadge({ status }: { status: Status }) {
  return (
    <span className={`inline-block rounded-md border px-2 py-0.5 text-xs font-medium ${STATUS_STYLE[status]}`}>
      {status}
    </span>
  );
}

// --------------------------------------------------------------------------
// Metrics bar — counts per status + MTTR + queue health + simulate button
// --------------------------------------------------------------------------
function fmtMTTR(seconds: number): string {
  if (!seconds) return "—";
  const m = Math.floor(seconds / 60);
  const s = Math.round(seconds % 60);
  if (m < 60) return `${m}m ${s}s`;
  const h = Math.floor(m / 60);
  return `${h}h ${m % 60}m`;
}

function MetricsBar() {
  const [stats, setStats] = useState<Stats | null>(null);
  const [health, setHealth] = useState<Health | null>(null);
  const [series, setSeries] = useState<TimeBucket[]>([]);
  const [busy, setBusy] = useState(false);

  // Poll stats + health + per-minute timeseries every 3s. All three endpoints
  // are O(microseconds) (Postgres aggregate, in-memory map, Redis HGETALL).
  useEffect(() => {
    const tick = async () => {
      try {
        const [s, h, ts] = await Promise.all([
          api.stats(),
          api.health(),
          api.timeseries("signals", 3600),
        ]);
        setStats(s);
        setHealth(h);
        setSeries(ts);
      } catch {
        /* tolerate transient backend hiccups */
      }
    };
    tick();
    const id = window.setInterval(tick, 3000);
    return () => window.clearInterval(id);
  }, []);

  const onSimulate = async () => {
    setBusy(true);
    try {
      await api.simulate(2000, 10);
    } finally {
      window.setTimeout(() => setBusy(false), 800);
    }
  };

  const totalLastHour = series.reduce((a, b) => a + b.count, 0);

  return (
    <div className="grid grid-cols-2 gap-3 md:grid-cols-7">
      <Tile label="Open"          value={stats?.by_status?.OPEN ?? 0}          accent="text-red-300" />
      <Tile label="Investigating" value={stats?.by_status?.INVESTIGATING ?? 0} accent="text-amber-300" />
      <Tile label="Resolved"      value={stats?.by_status?.RESOLVED ?? 0}      accent="text-emerald-300" />
      <Tile label="Closed"        value={stats?.by_status?.CLOSED ?? 0}        accent="text-slate-300" />
      <Tile label="Avg MTTR"      value={fmtMTTR(stats?.avg_mttr_seconds ?? 0)} accent="text-sky-300" />
      <Tile
        label={`Queue ${health ? `${health.queue_depth}/${health.queue_capacity}` : "—"}`}
        value={health?.status ?? "…"}
        accent={health?.status === "ok" ? "text-emerald-300" : "text-amber-300"}
      />
      <SparklineTile label="Signals (1h)" value={totalLastHour} series={series} />
      <button
        onClick={onSimulate}
        disabled={busy}
        className="col-span-2 rounded-lg bg-indigo-500/20 px-3 py-2 text-sm font-medium text-indigo-200 ring-1 ring-indigo-500/40 hover:bg-indigo-500/30 disabled:opacity-50 md:col-span-7"
      >
        {busy ? "Generating storm…" : "🚨 Simulate signal storm (2k/sec × 10s)"}
      </button>
    </div>
  );
}

function Tile({ label, value, accent }: { label: string; value: string | number; accent: string }) {
  return (
    <div className="rounded-xl border border-slate-800 bg-slate-900/60 p-3">
      <div className="text-[11px] uppercase tracking-wide text-slate-400">{label}</div>
      <div className={`mt-1 text-xl font-semibold ${accent}`}>{value}</div>
    </div>
  );
}

// --------------------------------------------------------------------------
// SparklineTile — renders the per-minute signal volume from the timeseries
// store (Redis hash buckets) as an inline SVG. Demonstrates the fourth
// storage sink ("aggregations") in the system architecture.
// --------------------------------------------------------------------------
function SparklineTile({
  label,
  value,
  series,
}: {
  label: string;
  value: number;
  series: TimeBucket[];
}) {
  const W = 120, H = 36;
  let path = "";
  if (series.length > 0) {
    const max = Math.max(1, ...series.map((b) => b.count));
    const step = series.length > 1 ? W / (series.length - 1) : 0;
    path = series
      .map((b, i) => {
        const x = i * step;
        const y = H - (b.count / max) * (H - 4) - 2;
        return `${i === 0 ? "M" : "L"}${x.toFixed(1)},${y.toFixed(1)}`;
      })
      .join(" ");
  }
  return (
    <div className="rounded-xl border border-slate-800 bg-slate-900/60 p-3">
      <div className="text-[11px] uppercase tracking-wide text-slate-400">{label}</div>
      <div className="mt-1 flex items-center justify-between gap-2">
        <div className="text-xl font-semibold text-indigo-300">{value}</div>
        <svg viewBox={`0 0 ${W} ${H}`} className="h-7 w-24 text-indigo-400">
          {path ? (
            <path d={path} fill="none" stroke="currentColor" strokeWidth="1.5" />
          ) : (
            <line x1="0" y1={H - 2} x2={W} y2={H - 2} stroke="currentColor" strokeWidth="0.5" opacity="0.3" />
          )}
        </svg>
      </div>
    </div>
  );
}

// --------------------------------------------------------------------------
// Live feed — list of active incidents, sorted by severity then recency
// --------------------------------------------------------------------------
function LiveFeed({
  items,
  selectedId,
  onSelect,
}: {
  items: WorkItem[];
  selectedId: string | null;
  onSelect: (id: string) => void;
}) {
  // Free-text filter — case-insensitive substring match against the three
  // fields operators actually scan visually (title, component_type, component_id).
  const [query, setQuery] = useState("");
  const q = query.trim().toLowerCase();

  const matchedAndSorted = useMemo(() => {
    const filtered = q
      ? items.filter(
          (w) =>
            w.title.toLowerCase().includes(q) ||
            w.component_type.toLowerCase().includes(q) ||
            w.component_id.toLowerCase().includes(q),
        )
      : items;

    return [...filtered].sort((a, b) => {
      if (SEVERITY_RANK[a.severity] !== SEVERITY_RANK[b.severity])
        return SEVERITY_RANK[a.severity] - SEVERITY_RANK[b.severity];
      return new Date(b.created_at).getTime() - new Date(a.created_at).getTime();
    });
  }, [items, q]);

  return (
    <div className="flex h-full flex-col">
      <div className="border-b border-slate-800 px-4 py-3">
        <div className="flex items-center justify-between">
          <h2 className="text-sm font-semibold uppercase tracking-wide text-slate-300">Live Feed</h2>
          <span className="text-xs text-slate-500">
            {q ? `${matchedAndSorted.length} / ${items.length}` : `${items.length} active`}
          </span>
        </div>
        {/* Search bar — filters by title / component_type / component_id.
            Useful when long-running outages produce many ×100 chunks for the
            same component (debouncer caps each window at 100 signals, so a
            sustained burst spawns multiple Work Items for that component). */}
        <div className="relative mt-2">
          <input
            type="text"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="Search component, type, title…"
            className="w-full rounded-md border border-slate-700 bg-slate-900/60 py-1.5 pl-8 pr-7 text-xs text-slate-100 placeholder-slate-500 outline-none focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500"
          />
          <svg
            className="pointer-events-none absolute left-2 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-slate-500"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
            strokeLinecap="round"
          >
            <circle cx="11" cy="11" r="7" />
            <path d="m20 20-3.5-3.5" />
          </svg>
          {query && (
            <button
              onClick={() => setQuery("")}
              className="absolute right-2 top-1/2 -translate-y-1/2 text-slate-500 hover:text-slate-200"
              aria-label="clear search"
            >
              ×
            </button>
          )}
        </div>
      </div>

      <div className="flex-1 overflow-y-auto">
        {matchedAndSorted.length === 0 && (
          <div className="px-4 py-12 text-center text-sm text-slate-500">
            {q ? (
              <>
                No incidents match <span className="font-mono text-slate-300">"{query}"</span>.
              </>
            ) : (
              <>
                No incidents yet. Click <span className="text-slate-300">Simulate</span> to generate some.
              </>
            )}
          </div>
        )}
        {matchedAndSorted.map((wi) => (
          <button
            key={wi.work_item_id}
            onClick={() => onSelect(wi.work_item_id)}
            className={`block w-full border-b border-slate-800/50 px-4 py-3 text-left transition hover:bg-slate-900/60 ${
              selectedId === wi.work_item_id ? "bg-slate-900/80 ring-1 ring-inset ring-indigo-500/30" : ""
            }`}
          >
            <div className="flex items-center justify-between gap-2">
              <div className="flex items-center gap-2">
                <SeverityBadge severity={wi.severity} />
                <StatusBadge status={wi.status} />
              </div>
              <span className="text-xs text-slate-500">×{wi.signal_count}</span>
            </div>
            <div className="mt-2 truncate text-sm font-medium text-slate-100">{wi.title}</div>
            <div className="mt-1 flex items-center gap-2 font-mono text-[11px] text-slate-500">
              <span>{wi.component_type}</span>
              <span>•</span>
              <span className="truncate">{wi.component_id}</span>
            </div>
          </button>
        ))}
      </div>
    </div>
  );
}

// --------------------------------------------------------------------------
// NotificationBell — header bell + dropdown of recent alerts.
//
// Backend wiring: AlertStrategy.fire() pushes a NotificationDict into the
// in-memory NotificationStore (see backend/app/notifications.py), which
// then broadcasts a {event:"notification"} WebSocket frame. App.tsx
// hands those frames to this component via the `registerPush` callback.
//
// REST fallback: on mount we load the last 50 from /notifications so the
// bell is populated even if the WS connection just opened.
// --------------------------------------------------------------------------
function NotificationBell({
  onSelectIncident,
  registerPush,
}: {
  onSelectIncident: (id: string) => void;
  registerPush: (fn: (n: Notification) => void) => void;
}) {
  const [items, setItems] = useState<Notification[]>([]);
  const [open, setOpen] = useState(false);

  // Initial fetch + register the live-push callback.
  useEffect(() => {
    api.notifications.list(50).then(setItems).catch(() => undefined);
    registerPush((n) => setItems((prev) => [n, ...prev].slice(0, 50)));
  }, [registerPush]);

  const unread = items.filter((n) => !n.read).length;

  const onClickItem = async (n: Notification) => {
    onSelectIncident(n.work_item_id);
    setOpen(false);
    if (!n.read) {
      try {
        await api.notifications.ack(n.id);
        setItems((prev) =>
          prev.map((x) => (x.id === n.id ? { ...x, read: true } : x)),
        );
      } catch {
        /* ignore ack failures — UI state already updated optimistically */
      }
    }
  };

  const onAckAll = async () => {
    try {
      await api.notifications.ackAll();
      setItems((prev) => prev.map((x) => ({ ...x, read: true })));
    } catch {
      /* ignore */
    }
  };

  return (
    <div className="relative">
      <button
        onClick={() => setOpen((v) => !v)}
        className="relative flex h-9 w-9 shrink-0 items-center justify-center rounded-lg border border-slate-700 bg-slate-900/60 text-slate-300 hover:border-indigo-500 hover:text-indigo-300"
        aria-label="notifications"
      >
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" className="h-4 w-4">
          <path d="M6 8a6 6 0 0 1 12 0c0 7 3 9 3 9H3s3-2 3-9" />
          <path d="M10.3 21a1.94 1.94 0 0 0 3.4 0" />
        </svg>
        {unread > 0 && (
          <span className="absolute -right-1 -top-1 flex h-4 min-w-[16px] items-center justify-center rounded-full bg-rose-500 px-1 text-[10px] font-semibold text-white">
            {unread > 99 ? "99+" : unread}
          </span>
        )}
      </button>

      {open && (
        <>
          {/* Click-outside catcher */}
          <div className="fixed inset-0 z-40" onClick={() => setOpen(false)} />
          <div className="absolute right-0 z-50 mt-2 w-[360px] max-w-[90vw] overflow-hidden rounded-xl border border-slate-700 bg-slate-950 shadow-2xl">
            <div className="flex items-center justify-between border-b border-slate-800 px-3 py-2">
              <span className="text-xs font-semibold uppercase tracking-wide text-slate-300">
                Notifications
              </span>
              <button
                onClick={onAckAll}
                disabled={unread === 0}
                className="rounded-md px-2 py-0.5 text-[11px] text-slate-400 hover:bg-slate-800 hover:text-slate-200 disabled:opacity-40"
              >
                mark all read
              </button>
            </div>
            <div className="max-h-[60vh] overflow-y-auto">
              {items.length === 0 && (
                <div className="px-3 py-8 text-center text-xs text-slate-500">
                  No alerts yet. Click <span className="text-slate-300">Simulate</span> to generate some.
                </div>
              )}
              {items.map((n) => (
                <button
                  key={n.id}
                  onClick={() => onClickItem(n)}
                  className={`block w-full border-b border-slate-800/50 px-3 py-2 text-left transition hover:bg-slate-900/60 ${
                    n.read ? "opacity-60" : ""
                  }`}
                >
                  <div className="flex items-center justify-between gap-2">
                    <SeverityBadge severity={n.severity} />
                    <span className="font-mono text-[10px] text-slate-500">
                      {fmtRelative(n.timestamp)}
                    </span>
                  </div>
                  <div className="mt-1 truncate text-sm text-slate-100">{n.title}</div>
                  <div className="mt-0.5 text-[11px] text-slate-500">
                    via <span className="text-slate-300">{n.channel}</span>
                  </div>
                </button>
              ))}
            </div>
          </div>
        </>
      )}
    </div>
  );
}

// "12 s ago" / "3 m ago" / "1 h ago" — keeps the dropdown compact.
function fmtRelative(unixSeconds: number): string {
  const diff = Math.max(0, Date.now() / 1000 - unixSeconds);
  if (diff < 60) return `${Math.floor(diff)}s ago`;
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
  if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`;
  return `${Math.floor(diff / 86400)}d ago`;
}

// --------------------------------------------------------------------------
// Top-level App
// --------------------------------------------------------------------------
export default function App() {
  const [items, setItems] = useState<WorkItem[]>([]);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [refreshKey, setRefreshKey] = useState(0);

  const refresh = useCallback(async () => {
    try {
      setItems(await api.listIncidents());
      setRefreshKey((k) => k + 1);
    } catch {
      /* ignore */
    }
  }, []);

  // Initial load + cheap polling fallback. WebSocket below is the primary
  // path for liveness; polling just guarantees recovery if a frame is lost.
  useEffect(() => {
    refresh();
    const id = window.setInterval(refresh, 5000);
    return () => window.clearInterval(id);
  }, [refresh]);

  // Notification stream — kept in a ref so NotificationBell can consume
  // incoming events without re-rendering App on every push.
  const notifPushRef = useRef<((n: Notification) => void) | null>(null);

  // Real-time push from the backend's WorkflowEngine — see workflow.py.
  // The hook is fully typed (WSMessage discriminated union), so the
  // narrowing in the switch below is exhaustive at compile time.
  useIncidentWebSocket((msg) => {
    if (msg.event === "work_item_created" || msg.event === "work_item_updated") {
      setItems((prev) => {
        const w = msg.data;
        const idx = prev.findIndex((x) => x.work_item_id === w.work_item_id);
        if (idx === -1) return [w, ...prev];
        const next = prev.slice();
        next[idx] = w;
        return next;
      });
    } else if (msg.event === "notification") {
      notifPushRef.current?.(msg.data);
    }
  });

  // ───── Responsive layout ─────────────────────────────────────────────────
  // Tailwind breakpoint `md` = 768px.
  //   • md and up : two-pane (feed | detail) side by side, classic desktop.
  //   • below md  : single pane that toggles between feed and detail. Phone
  //                 users tap an incident → detail takes over the screen,
  //                 a "← back" button returns to the feed.
  return (
    <div className="flex h-screen flex-col">
      <header className="border-b border-slate-800 bg-slate-950/80 px-4 py-3 backdrop-blur sm:px-6">
        <div className="flex items-center gap-3">
          <div className="flex h-8 w-8 shrink-0 items-center justify-center rounded-lg bg-indigo-500/20 text-indigo-300">
            <svg viewBox="0 0 24 24" fill="currentColor" className="h-4 w-4">
              <path d="M13 2L3 14h7l-1 8 11-14h-7l1-6z" />
            </svg>
          </div>
          <div className="min-w-0 flex-1">
            <h1 className="truncate text-sm font-semibold tracking-tight sm:text-base">
              Incident Management System
            </h1>
            <p className="hidden text-xs text-slate-400 sm:block">
              Mission-critical observability — Zeotap SRE assignment
            </p>
          </div>
          {/* Notification bell — fed by AlertStrategy.fire() on the backend.
              Bell badge updates live via WebSocket; clicking opens a dropdown
              with the most recent 50 alerts. */}
          <NotificationBell
            onSelectIncident={setSelectedId}
            registerPush={(fn) => (notifPushRef.current = fn)}
          />
        </div>
      </header>

      <div className="border-b border-slate-800 bg-slate-950 p-3 sm:p-4">
        <MetricsBar />
      </div>

      <main className="flex flex-1 overflow-hidden">
        {/* ── Live Feed ───────────────────────────────────────────────── */}
        {/* Mobile: full width; hidden once an incident is opened.        */}
        {/* Desktop: fixed-width sidebar, always visible.                 */}
        <aside
          className={`${
            selectedId ? "hidden md:block" : "block"
          } w-full border-r border-slate-800 bg-slate-950 md:w-[320px] lg:w-[360px]`}
        >
          <LiveFeed items={items} selectedId={selectedId} onSelect={setSelectedId} />
        </aside>

        {/* ── Incident Detail ─────────────────────────────────────────── */}
        {/* Mobile: full width when an incident is selected.              */}
        {/* Desktop: fills remaining space.                               */}
        <section
          className={`${
            selectedId ? "block" : "hidden md:block"
          } flex-1 bg-slate-950/40`}
        >
          {selectedId ? (
            <div className="flex h-full flex-col">
              {/* Mobile-only "back to feed" bar */}
              <button
                onClick={() => setSelectedId(null)}
                className="flex items-center gap-2 border-b border-slate-800 px-4 py-2 text-xs text-slate-300 hover:bg-slate-900 md:hidden"
              >
                <span aria-hidden>←</span> Back to feed
              </button>
              <div className="flex-1 overflow-hidden">
                <IncidentDetailView
                  incidentId={selectedId}
                  refreshKey={refreshKey}
                  onChanged={refresh}
                />
              </div>
            </div>
          ) : (
            <div className="flex h-full items-center justify-center px-6 text-center text-sm text-slate-500">
              Select an incident from the live feed →
            </div>
          )}
        </section>
      </main>
    </div>
  );
}
