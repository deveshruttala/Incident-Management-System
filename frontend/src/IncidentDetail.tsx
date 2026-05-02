// Incident detail pane + RCA submission form.
// Kept in one file because the two components are always shown together
// and share the same data fetch.

import { useEffect, useState } from "react";
import { api, ApiError } from "./api";
import type { IncidentDetail, RawSignal, RCA, RootCauseCategory, Status } from "./api";
import { SeverityBadge, StatusBadge } from "./App";

const CATEGORIES: RootCauseCategory[] = [
  "INFRASTRUCTURE",
  "DEPLOYMENT",
  "CONFIG_CHANGE",
  "DEPENDENCY_FAILURE",
  "CODE_DEFECT",
  "CAPACITY",
  "HUMAN_ERROR",
  "UNKNOWN",
];

// State machine for the "Next →" buttons. Mirrors backend `WorkItemState`.
// CLOSED → OPEN is a "reopen" — used when a fix didn't actually stick.
const NEXT_STATES: Record<Status, Status[]> = {
  OPEN: ["INVESTIGATING", "RESOLVED"],
  INVESTIGATING: ["RESOLVED", "OPEN"],
  RESOLVED: ["INVESTIGATING", "CLOSED"],
  CLOSED: ["OPEN"],
};

// Human-friendly button label. The reopen edge gets its own copy so it's
// obvious what's about to happen.
const TRANSITION_LABEL: Record<Status, Partial<Record<Status, string>>> = {
  OPEN:          { INVESTIGATING: "→ INVESTIGATING", RESOLVED: "→ RESOLVED" },
  INVESTIGATING: { RESOLVED: "→ RESOLVED", OPEN: "↺ Re-open" },
  RESOLVED:      { INVESTIGATING: "↺ Re-investigate", CLOSED: "→ CLOSED" },
  CLOSED:        { OPEN: "↺ Re-open incident" },
};

// `<input type="datetime-local">` wants `YYYY-MM-DDTHH:MM` in *local* time.
function isoLocal(value: string | null | undefined): string {
  const d = value ? new Date(value) : new Date();
  const pad = (n: number) => n.toString().padStart(2, "0");
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`;
}

// --------------------------------------------------------------------------
// IncidentDetailView — left half: raw signals, right half: RCA form
// --------------------------------------------------------------------------
export function IncidentDetailView({
  incidentId,
  refreshKey,
  onChanged,
}: {
  incidentId: string;
  refreshKey: number;
  onChanged: () => void;
}) {
  const [data, setData] = useState<IncidentDetail | null>(null);
  const [error, setError] = useState<ApiError | Error | null>(null);

  useEffect(() => {
    let cancelled = false;
    setError(null);
    api
      .getIncident(incidentId)
      .then((d) => !cancelled && setData(d))
      .catch((e: unknown) => !cancelled && setError(toError(e)));
    return () => {
      cancelled = true;
    };
  }, [incidentId, refreshKey]);

  const transition = async (target: Status) => {
    try {
      await api.transition(incidentId, target);
      setError(null);
      onChanged();
    } catch (e: unknown) {
      setError(toError(e));
    }
  };

  // Initial load failure: nothing to show, just the banner.
  if (error && !data)
    return (
      <div className="p-4">
        <ErrorBanner error={error} onDismiss={() => setError(null)} />
      </div>
    );
  if (!data) return <div className="p-6 text-slate-500">Loading…</div>;

  const wi = data.work_item;

  return (
    <div className="flex h-full flex-col overflow-y-auto">
      {/* Transition / load error → shown above the header without nuking
          the rest of the view. */}
      {error && (
        <div className="px-6 pt-4">
          <ErrorBanner error={error} onDismiss={() => setError(null)} />
        </div>
      )}

      <div className="border-b border-slate-800 px-6 py-4">
        <div className="flex items-center gap-2">
          <SeverityBadge severity={wi.severity} />
          <StatusBadge status={wi.status} />
          <span className="ml-auto font-mono text-xs text-slate-500">
            {wi.work_item_id.slice(0, 12)}…
          </span>
        </div>
        <h1 className="mt-2 text-lg font-semibold text-slate-100">{wi.title}</h1>
        <div className="mt-1 font-mono text-xs text-slate-500">
          {wi.component_type} • {wi.component_id} • {wi.signal_count} signals
          {wi.mttr_seconds != null && ` • MTTR ${Math.round(wi.mttr_seconds)}s`}
        </div>

        <div className="mt-3 flex flex-wrap gap-2">
          {NEXT_STATES[wi.status].map((s) => {
            const isReopen = wi.status === "CLOSED" && s === "OPEN";
            return (
              <button
                key={s}
                onClick={() => transition(s)}
                className={`rounded-md border px-2.5 py-1 text-xs ${
                  isReopen
                    ? "border-amber-500/40 bg-amber-500/10 text-amber-200 hover:bg-amber-500/20"
                    : "border-slate-600 hover:bg-slate-800"
                }`}
              >
                {TRANSITION_LABEL[wi.status]?.[s] ?? `→ ${s}`}
              </button>
            );
          })}
        </div>
      </div>

      <div className="grid flex-1 grid-cols-1 gap-4 p-6 lg:grid-cols-2">
        <div>
          <h2 className="mb-2 text-sm font-semibold uppercase tracking-wide text-slate-300">
            Raw signals (NoSQL)
          </h2>
          <div className="max-h-[60vh] space-y-2 overflow-y-auto rounded-xl border border-slate-800 bg-slate-900/40 p-3">
            {data.signals.length === 0 && (
              <div className="text-sm text-slate-500">No raw signals stored.</div>
            )}
            {data.signals.map((s: RawSignal, i: number) => (
              <div
                key={s._id ?? s.signal_id ?? `signal-${i}`}
                className="rounded-md border border-slate-800/70 bg-slate-950/40 p-2 text-xs"
              >
                <div className="flex items-center justify-between">
                  <span className="font-mono text-slate-400">
                    {(s.received_at ?? s.occurred_at ?? "").slice(11, 19)}
                  </span>
                  {s.error_code && (
                    <span className="rounded bg-rose-500/15 px-1.5 py-0.5 font-mono text-[10px] text-rose-300">
                      {s.error_code}
                    </span>
                  )}
                </div>
                <div className="mt-1 text-slate-200">{s.message}</div>
                {s.latency_ms != null && (
                  <div className="mt-1 font-mono text-[11px] text-slate-500">
                    latency {Math.round(s.latency_ms)} ms
                  </div>
                )}
              </div>
            ))}
          </div>
        </div>
        <div>
          <RCAForm detail={data} onSaved={onChanged} />
        </div>
      </div>
    </div>
  );
}

// --------------------------------------------------------------------------
// RCAForm — fills out / updates the RCA, optional one-click "save & close"
// --------------------------------------------------------------------------
function RCAForm({ detail, onSaved }: { detail: IncidentDetail; onSaved: () => void }) {
  const wi = detail.work_item;
  const existing = detail.rca;

  const [start, setStart] = useState(isoLocal(existing?.incident_start ?? wi.start_time));
  const [end, setEnd] = useState(isoLocal(existing?.incident_end ?? new Date().toISOString()));
  const [category, setCategory] = useState<RootCauseCategory>(
    existing?.root_cause_category ?? "INFRASTRUCTURE"
  );
  const [fix, setFix] = useState(existing?.fix_applied ?? "");
  const [prevention, setPrevention] = useState(existing?.prevention_steps ?? "");
  const [submittedBy, setSubmittedBy] = useState(existing?.submitted_by ?? "");
  const [error, setError] = useState<ApiError | Error | null>(null);
  const [busy, setBusy] = useState(false);

  // Reset the form when the user picks a different incident.
  useEffect(() => {
    setStart(isoLocal(existing?.incident_start ?? wi.start_time));
    setEnd(isoLocal(existing?.incident_end ?? new Date().toISOString()));
    setCategory(existing?.root_cause_category ?? "INFRASTRUCTURE");
    setFix(existing?.fix_applied ?? "");
    setPrevention(existing?.prevention_steps ?? "");
    setSubmittedBy(existing?.submitted_by ?? "");
    setError(null);
  }, [wi.work_item_id]);

  // Lightweight client-side hint so the user gets immediate feedback without
  // hitting the network. The server still does the authoritative validation.
  const fixTooShort = fix.trim().length < 10;
  const preventionTooShort = prevention.trim().length < 10;

  const submit = async (closeAfter: boolean) => {
    setBusy(true);
    setError(null);
    try {
      const rca: RCA = {
        work_item_id: wi.work_item_id,
        incident_start: new Date(start).toISOString(),
        incident_end: new Date(end).toISOString(),
        root_cause_category: category,
        fix_applied: fix.trim(),
        prevention_steps: prevention.trim(),
        submitted_by: submittedBy.trim() || undefined,
      };
      await api.submitRCA(wi.work_item_id, rca);
      // "Save & close" flow walks through the legal state transitions for us.
      if (closeAfter) {
        if (wi.status === "OPEN" || wi.status === "INVESTIGATING") {
          await api.transition(wi.work_item_id, "RESOLVED");
        }
        await api.transition(wi.work_item_id, "CLOSED");
      }
      onSaved();
    } catch (e: unknown) {
      setError(toError(e));
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="space-y-4 rounded-xl border border-slate-800 bg-slate-900/40 p-4">
      <div>
        <h3 className="text-sm font-semibold uppercase tracking-wide text-slate-300">
          Root Cause Analysis
        </h3>
        <p className="mt-1 text-xs text-slate-500">
          A complete RCA is required before this incident can be CLOSED.
        </p>
      </div>

      <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
        <Field label="Incident start">
          <input type="datetime-local" value={start} onChange={(e) => setStart(e.target.value)} className="input" />
        </Field>
        <Field label="Incident end">
          <input type="datetime-local" value={end} onChange={(e) => setEnd(e.target.value)} className="input" />
        </Field>
      </div>

      <Field label="Root cause category">
        <select value={category} onChange={(e) => setCategory(e.target.value as RootCauseCategory)} className="input">
          {CATEGORIES.map((c) => (
            <option key={c} value={c}>
              {c.replace("_", " ")}
            </option>
          ))}
        </select>
      </Field>

      <Field
        label="Fix applied"
        hint={
          fix.length === 0
            ? "Required · at least 10 characters"
            : fixTooShort
            ? `${10 - fix.trim().length} more character(s) needed`
            : `${fix.trim().length} characters ✓`
        }
        hintTone={fix.length === 0 ? "muted" : fixTooShort ? "warn" : "ok"}
      >
        <textarea
          value={fix}
          onChange={(e) => setFix(e.target.value)}
          rows={3}
          placeholder="What was done to mitigate? (≥10 chars)"
          className={`input ${fix.length > 0 && fixTooShort ? "input-warn" : ""}`}
        />
      </Field>

      <Field
        label="Prevention steps"
        hint={
          prevention.length === 0
            ? "Required · at least 10 characters"
            : preventionTooShort
            ? `${10 - prevention.trim().length} more character(s) needed`
            : `${prevention.trim().length} characters ✓`
        }
        hintTone={prevention.length === 0 ? "muted" : preventionTooShort ? "warn" : "ok"}
      >
        <textarea
          value={prevention}
          onChange={(e) => setPrevention(e.target.value)}
          rows={3}
          placeholder="How will this be prevented going forward? (≥10 chars)"
          className={`input ${prevention.length > 0 && preventionTooShort ? "input-warn" : ""}`}
        />
      </Field>

      <Field label="Submitted by (optional)">
        <input
          value={submittedBy}
          onChange={(e) => setSubmittedBy(e.target.value)}
          placeholder="oncall@zeotap"
          className="input"
        />
      </Field>

      {error && <ErrorBanner error={error} onDismiss={() => setError(null)} />}

      <div className="flex flex-wrap items-center gap-2">
        <button
          onClick={() => submit(false)}
          disabled={busy}
          className="rounded-lg border border-slate-600 px-3 py-2 text-sm hover:bg-slate-800 disabled:opacity-50"
        >
          {busy ? "Saving…" : "Save RCA"}
        </button>
        <button
          onClick={() => submit(true)}
          disabled={busy || wi.status === "CLOSED"}
          className="rounded-lg bg-emerald-500/20 px-3 py-2 text-sm font-medium text-emerald-200 ring-1 ring-emerald-500/40 hover:bg-emerald-500/30 disabled:opacity-50"
        >
          {wi.status === "CLOSED" ? "Already closed" : "Save & Close incident"}
        </button>
      </div>

      {/* Local form styling — kept inline since these classes are only
          used by this component. Tailwind's @apply would be overkill. */}
      <style>{`
        .input {
          width: 100%;
          background: rgba(15, 23, 42, 0.6);
          border: 1px solid rgb(51 65 85);
          color: rgb(226 232 240);
          border-radius: 0.5rem;
          padding: 0.5rem 0.75rem;
          font-size: 0.875rem;
          outline: none;
          font-family: inherit;
          transition: border-color .15s, box-shadow .15s;
        }
        .input:focus { border-color: rgb(99 102 241); box-shadow: 0 0 0 1px rgb(99 102 241); }
        .input-warn  { border-color: rgb(245 158 11); }
        .input-warn:focus { border-color: rgb(245 158 11); box-shadow: 0 0 0 1px rgb(245 158 11); }
        textarea.input { font-family: ui-monospace, monospace; }
      `}</style>
    </div>
  );
}

function Field({
  label,
  hint,
  hintTone = "muted",
  children,
}: {
  label: string;
  hint?: string;
  hintTone?: "ok" | "warn" | "muted";
  children: React.ReactNode;
}) {
  const toneClass =
    hintTone === "ok" ? "text-emerald-400" :
    hintTone === "warn" ? "text-amber-400" :
    "text-slate-500";
  return (
    <label className="block">
      <div className="mb-1 flex items-center justify-between">
        <span className="text-xs font-medium uppercase tracking-wide text-slate-400">{label}</span>
        {hint && <span className={`text-[10px] font-mono ${toneClass}`}>{hint}</span>}
      </div>
      {children}
    </label>
  );
}

// Narrow an unknown caught value into something we can render. Promises and
// fetch can reject with anything; this gives us a single safe entry point.
function toError(e: unknown): ApiError | Error {
  if (e instanceof ApiError) return e;
  if (e instanceof Error) return e;
  return new Error(typeof e === "string" ? e : JSON.stringify(e));
}

// --------------------------------------------------------------------------
// ErrorBanner — turns ApiError into a friendly card with per-field issues.
// --------------------------------------------------------------------------
function ErrorBanner({
  error,
  onDismiss,
}: {
  error: ApiError | Error;
  onDismiss?: () => void;
}) {
  const isApi = error instanceof ApiError;
  const status = isApi ? error.status : 0;
  const title = isApi ? error.title : "Request failed";
  const detail = isApi ? error.detail : error.message;
  const issues = isApi ? error.issues : [];

  const accent =
    status === 422 || status === 400 ? "amber" :
    status === 401 || status === 403 ? "rose" :
    status === 409 ? "indigo" :
    status === 429 ? "amber" :
    status === 404 ? "slate" :
    "rose";

  const colors: Record<string, string> = {
    amber:  "border-amber-500/40  bg-amber-500/10  text-amber-200",
    rose:   "border-rose-500/40   bg-rose-500/10   text-rose-200",
    indigo: "border-indigo-500/40 bg-indigo-500/10 text-indigo-200",
    slate:  "border-slate-500/40  bg-slate-500/10  text-slate-200",
  };

  return (
    <div className={`rounded-lg border ${colors[accent]} p-3 text-sm`}>
      <div className="flex items-start justify-between gap-3">
        <div className="flex-1">
          <div className="flex items-center gap-2">
            {status > 0 && (
              <span className="rounded bg-black/30 px-1.5 py-0.5 font-mono text-[10px]">
                {status}
              </span>
            )}
            <span className="font-semibold">{title}</span>
          </div>
          {issues.length > 0 ? (
            <ul className="mt-2 space-y-1 text-xs">
              {issues.map((iss, i) => (
                <li key={i} className="flex gap-2">
                  <span className="font-mono opacity-70">•</span>
                  <span>
                    <span className="font-semibold">{iss.field}</span>
                    <span className="opacity-80"> — {iss.message}</span>
                  </span>
                </li>
              ))}
            </ul>
          ) : (
            <div className="mt-1 text-xs opacity-90">{detail}</div>
          )}
        </div>
        {onDismiss && (
          <button
            onClick={onDismiss}
            aria-label="dismiss"
            className="-mt-1 rounded px-2 text-lg leading-none opacity-60 hover:opacity-100"
          >
            ×
          </button>
        )}
      </div>
    </div>
  );
}
