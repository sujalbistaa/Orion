import { useState } from "react";
import { Link } from "react-router-dom";
import { ApiError } from "../api/client";
import { listExperiments, listRuns, startRun } from "../api/resources";
import { useAsync } from "../hooks/useAsync";
import { ErrorState, LoadingState, EmptyState } from "../components/States";
import { StatusBadge } from "../components/StatusBadge";
import { Modal } from "../components/Modal";
import { useToast, describeError } from "../components/Toast";
import { formatDateTime, formatGoDuration } from "../utils/format";
import { runStateTone } from "../utils/status";
import type { ExperimentDescriptor } from "../api/types";

export function FaultInjection() {
  const experiments = useAsync(() => listExperiments(), []);
  const runs = useAsync(() => listRuns(), []);
  const toast = useToast();
  const [starting, setStarting] = useState<ExperimentDescriptor | null>(null);

  const disabled = experiments.error instanceof ApiError && experiments.error.status === 404;

  if (experiments.initialLoading) {
    return (
      <div className="page">
        <LoadingState label="Loading experiments…" />
      </div>
    );
  }

  if (disabled) {
    return (
      <div className="page">
        <div className="page-header">
          <div>
            <h1>Fault Injection</h1>
          </div>
        </div>
        <EmptyState
          title="Fault injection is not enabled on this server"
          detail="Start orion-server with -enable-fault-injection to run controlled failure experiments against this cluster."
        />
      </div>
    );
  }

  if (experiments.error) {
    return (
      <div className="page">
        <ErrorState error={experiments.error} onRetry={experiments.refetch} />
      </div>
    );
  }

  return (
    <div className="page">
      <div className="page-header">
        <div>
          <h1>Fault Injection</h1>
          <p className="page-subtitle">Controlled failure experiments that assert cluster invariants under real breakage.</p>
        </div>
        <div className="page-actions">
          <button
            className="btn"
            onClick={() => {
              experiments.refetch();
              runs.refetch();
            }}
          >
            Refresh
          </button>
        </div>
      </div>

      <div className="section">
        <div className="section-title">Available experiments</div>
        {(experiments.data?.items.length ?? 0) === 0 ? (
          <EmptyState title="No experiments registered" />
        ) : (
          <div style={{ display: "grid", gap: 10 }}>
            {experiments.data!.items.map((exp) => (
              <div className="panel" key={exp.kind}>
                <div className="panel-header">
                  <span>
                    {exp.name} {exp.destructive && <StatusBadge label="Destructive" tone="critical" />}
                  </span>
                  <button className="btn btn-sm btn-primary" onClick={() => setStarting(exp)}>
                    Run experiment
                  </button>
                </div>
                <div className="panel-body">
                  <p>{exp.description}</p>
                  <div className="hypothesis-box" style={{ marginTop: 10 }}>
                    <strong>Hypothesis:</strong> {exp.hypothesis}
                  </div>
                  {exp.invariants.length > 0 && (
                    <div style={{ marginTop: 10 }}>
                      <div className="text-muted" style={{ marginBottom: 4, fontWeight: 600, fontSize: 12 }}>
                        Invariants asserted
                      </div>
                      <ul style={{ margin: 0, paddingLeft: 18 }}>
                        {exp.invariants.map((inv) => (
                          <li key={inv}>{inv}</li>
                        ))}
                      </ul>
                    </div>
                  )}
                </div>
              </div>
            ))}
          </div>
        )}
      </div>

      <div className="section">
        <div className="section-title">Runs</div>
        {runs.error && <ErrorState error={runs.error} onRetry={runs.refetch} />}
        {!runs.error && runs.initialLoading && <LoadingState label="Loading runs…" />}
        {!runs.error && !runs.initialLoading && (runs.data?.items.length ?? 0) === 0 && (
          <EmptyState title="No runs yet" detail="Start an experiment above to see its run here." />
        )}
        {!runs.error && !runs.initialLoading && (runs.data?.items.length ?? 0) > 0 && (
          <div className="table-wrap">
            <table className="data-table">
              <thead>
                <tr>
                  <th>ID</th>
                  <th>Kind</th>
                  <th>State</th>
                  <th>Started</th>
                  <th>Actor</th>
                  <th className="wrap">Recovery</th>
                </tr>
              </thead>
              <tbody>
                {runs.data!.items.map((r) => (
                  <tr key={r.id}>
                    <td className="mono">
                      <Link to={`/faults/runs/${encodeURIComponent(r.id)}`}>{r.id}</Link>
                    </td>
                    <td>{r.kind}</td>
                    <td>
                      <StatusBadge label={r.state} tone={runStateTone(r.state)} />
                    </td>
                    <td>{formatDateTime(r.startedAt)}</td>
                    <td>{r.actor}</td>
                    <td className="wrap">{r.recoveryDuration ? formatGoDuration(r.recoveryDuration) : "—"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      {starting && (
        <StartRunModal
          experiment={starting}
          onClose={() => setStarting(null)}
          onStarted={(id) => {
            setStarting(null);
            runs.refetch();
            toast.push("success", `Experiment run ${id} started`);
          }}
        />
      )}
    </div>
  );
}

function StartRunModal({
  experiment,
  onClose,
  onStarted,
}: {
  experiment: ExperimentDescriptor;
  onClose: () => void;
  onStarted: (id: string) => void;
}) {
  const [params, setParams] = useState<Record<string, string>>(() => {
    const initial: Record<string, string> = {};
    for (const p of experiment.parameters) initial[p.name] = p.default ?? "";
    return initial;
  });
  const [duration, setDuration] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function submit() {
    setBusy(true);
    setError(null);
    try {
      const missing = experiment.parameters.filter((p) => p.required && !params[p.name]?.trim());
      if (missing.length > 0) {
        setError(`Missing required parameter(s): ${missing.map((m) => m.name).join(", ")}`);
        setBusy(false);
        return;
      }
      const run = await startRun({
        kind: experiment.kind,
        params,
        durationSeconds: duration ? Number(duration) : undefined,
      });
      onStarted(run.id);
    } catch (err) {
      setError(describeError(err));
    } finally {
      setBusy(false);
    }
  }

  return (
    <Modal title={`Run: ${experiment.name}`} onClose={onClose}>
      <div className="hypothesis-box" style={{ marginBottom: 14 }}>
        <strong>Hypothesis:</strong> {experiment.hypothesis}
      </div>
      {experiment.destructive && (
        <p className="text-critical" style={{ marginBottom: 14 }}>
          This experiment is destructive: it will stop real processes or connections in the cluster.
        </p>
      )}
      <form
        className="form-grid"
        onSubmit={(e) => {
          e.preventDefault();
          void submit();
        }}
      >
        {error && <div className="inline-error">{error}</div>}
        {experiment.parameters.map((p) => (
          <div className="field" key={p.name}>
            <label htmlFor={`param-${p.name}`}>
              {p.name} {p.required && <span className="text-critical">*</span>} <span className="text-faint">({p.type})</span>
            </label>
            <input
              id={`param-${p.name}`}
              type="text"
              required={p.required}
              value={params[p.name] ?? ""}
              onChange={(e) => setParams({ ...params, [p.name]: e.target.value })}
              placeholder={p.type === "duration" ? "e.g. 30s" : undefined}
            />
            <span className="hint">{p.help}</span>
          </div>
        ))}
        <div className="field">
          <label htmlFor="run-duration">Duration override (seconds, optional)</label>
          <input
            id="run-duration"
            type="number"
            min="0"
            value={duration}
            onChange={(e) => setDuration(e.target.value)}
            placeholder="Uses experiment default when empty"
          />
        </div>
        <div className="modal-footer" style={{ padding: 0, borderTop: "none", marginTop: 4 }}>
          <button type="button" className="btn" onClick={onClose} disabled={busy}>
            Cancel
          </button>
          <button type="submit" className={experiment.destructive ? "btn btn-danger" : "btn btn-primary"} disabled={busy}>
            {busy ? "Starting…" : "Start run"}
          </button>
        </div>
      </form>
    </Modal>
  );
}
