import { useEffect, useRef, useState } from "react";
import { Link, useParams } from "react-router-dom";
import { abortRun, getRun } from "../api/resources";
import { useAsync } from "../hooks/useAsync";
import { ErrorState, LoadingState } from "../components/States";
import { StatusBadge } from "../components/StatusBadge";
import { ConfirmDialog } from "../components/ConfirmDialog";
import { useToast, describeError } from "../components/Toast";
import { formatDateTime, formatGoDuration } from "../utils/format";
import { runStateTone } from "../utils/status";
import { RUN_TERMINAL_STATES } from "../api/types";

const POLL_MS = 2000;

export function FaultRunDetail() {
  const { id = "" } = useParams();
  const toast = useToast();
  const run = useAsync(() => getRun(id), [id]);
  const [confirmAbort, setConfirmAbort] = useState(false);
  const [busy, setBusy] = useState(false);
  const timerRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const isActive = run.data && !RUN_TERMINAL_STATES.includes(run.data.state);

  useEffect(() => {
    if (isActive) {
      timerRef.current = setInterval(() => run.refetch(), POLL_MS);
    }
    return () => {
      if (timerRef.current) clearInterval(timerRef.current);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isActive, id]);

  if (run.initialLoading) return <div className="page"><LoadingState label="Loading run…" /></div>;
  if (run.error) return <div className="page"><ErrorState error={run.error} onRetry={run.refetch} /></div>;
  if (!run.data) return null;

  const r = run.data;

  async function doAbort() {
    setBusy(true);
    try {
      await abortRun(r.id);
      toast.push("success", "Run aborted");
      setConfirmAbort(false);
      run.refetch();
    } catch (err) {
      toast.push("error", describeError(err));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="page">
      <div className="breadcrumb">
        <Link to="/faults">Fault Injection</Link> / {r.id}
      </div>
      <div className="page-header">
        <div>
          <h1>{r.kind}</h1>
          <p className="page-subtitle">
            <StatusBadge label={r.state} tone={runStateTone(r.state)} /> run {r.id} · started {formatDateTime(r.startedAt)} by{" "}
            {r.actor}
            {isActive && <span> · polling live</span>}
          </p>
        </div>
        <div className="page-actions">
          {isActive && (
            <button className="btn btn-danger" onClick={() => setConfirmAbort(true)}>
              Abort
            </button>
          )}
        </div>
      </div>

      {r.error && (
        <div className="section">
          <div className="state-block state-error">
            <div className="state-title">Run failed to execute</div>
            <div>{r.error}</div>
          </div>
        </div>
      )}

      <div className="section">
        <div className="section-title">Summary</div>
        <dl className="kv-grid">
          <dt>Params</dt>
          <dd className="mono">
            {Object.entries(r.params).length > 0
              ? Object.entries(r.params).map(([k, v]) => `${k}=${v}`).join(", ")
              : "none"}
          </dd>
          <dt>Affected nodes</dt>
          <dd>{r.affectedNodes && r.affectedNodes.length > 0 ? r.affectedNodes.join(", ") : "none"}</dd>
          <dt>Affected workloads</dt>
          <dd>{r.affectedWorkloads && r.affectedWorkloads.length > 0 ? r.affectedWorkloads.join(", ") : "none"}</dd>
          <dt>Finished</dt>
          <dd>{formatDateTime(r.finishedAt)}</dd>
          <dt>Recovery duration</dt>
          <dd>{r.recoveryDuration ? formatGoDuration(r.recoveryDuration) : "—"}</dd>
        </dl>
      </div>

      <div className="section">
        <div className="section-title">Invariants</div>
        {r.invariants.length === 0 ? (
          <p className="text-muted">No invariant checks recorded yet.</p>
        ) : (
          <div className="invariant-list">
            {r.invariants.map((inv) => (
              <div className="invariant-item" key={inv.name}>
                <StatusBadge label={inv.held ? "Held" : "Violated"} tone={inv.held ? "healthy" : "critical"} />
                <div>
                  <strong>{inv.name}</strong>
                  <div className="text-muted">{inv.detail}</div>
                  {inv.violations > 0 && <div className="text-critical">{inv.violations} violation(s) observed</div>}
                </div>
              </div>
            ))}
          </div>
        )}
      </div>

      <div className="section">
        <div className="section-title">Timeline</div>
        {r.timeline.length === 0 ? (
          <p className="text-muted">No timeline entries yet.</p>
        ) : (
          <div className="timeline">
            {r.timeline.map((t, i) => (
              <div className={`timeline-entry level-${t.level}`} key={i}>
                <span className="timeline-time">{t.elapsed}</span>
                <StatusBadge label={t.phase} tone={runStateTone(t.phase)} /> <span>{t.message}</span>
              </div>
            ))}
          </div>
        )}
      </div>

      {confirmAbort && (
        <ConfirmDialog
          title={`Abort run ${r.id}?`}
          danger
          confirmLabel="Abort run"
          busy={busy}
          onConfirm={doAbort}
          onCancel={() => setConfirmAbort(false)}
          body={<p>This lifts the injected fault and stops the experiment early.</p>}
        />
      )}
    </div>
  );
}
