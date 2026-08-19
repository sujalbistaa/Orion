import { useState } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";
import { deleteWorkload, getWorkload } from "../api/resources";
import { useAsync } from "../hooks/useAsync";
import { useWatchRefetch } from "../api/WatchProvider";
import { ErrorState, LoadingState } from "../components/States";
import { StatusBadge } from "../components/StatusBadge";
import { ConfirmDialog } from "../components/ConfirmDialog";
import { LogViewer } from "../components/LogViewer";
import { useToast, describeError } from "../components/Toast";
import { formatDateTime, formatGoDuration, formatResources } from "../utils/format";
import { healthTone, severityTone, workloadPhaseTone } from "../utils/status";

export function WorkloadDetail() {
  const { name = "" } = useParams();
  const navigate = useNavigate();
  const toast = useToast();
  const detail = useAsync(() => getWorkload(name), [name]);
  useWatchRefetch(["Workload", "Node"], detail.refetch);

  const [confirmDelete, setConfirmDelete] = useState(false);
  const [force, setForce] = useState(false);
  const [busy, setBusy] = useState(false);
  const [tab, setTab] = useState<"overview" | "logs" | "placement">("overview");

  if (detail.initialLoading) return <div className="page"><LoadingState label="Loading workload…" /></div>;
  if (detail.error) return <div className="page"><ErrorState error={detail.error} onRetry={detail.refetch} /></div>;
  if (!detail.data) return null;

  const { workload: w, node, events } = detail.data;

  async function doDelete() {
    setBusy(true);
    try {
      await deleteWorkload(w.name, { force });
      toast.push("success", `${w.name} deleted`);
      navigate("/workloads");
    } catch (err) {
      toast.push("error", describeError(err));
      setBusy(false);
    }
  }

  return (
    <div className="page">
      <div className="breadcrumb">
        <Link to="/workloads">Workloads</Link> / {w.name}
      </div>
      <div className="page-header">
        <div>
          <h1>{w.name}</h1>
          <p className="page-subtitle">
            <StatusBadge label={w.status.phase} tone={workloadPhaseTone(w.status.phase)} />{" "}
            <StatusBadge label={w.status.health} tone={healthTone(w.status.health)} />{" "}
            <span className="mono">{w.spec.image}</span>
          </p>
        </div>
        <div className="page-actions">
          <button className="btn btn-danger" onClick={() => setConfirmDelete(true)}>
            Delete
          </button>
        </div>
      </div>

      <div className="tabs" role="tablist">
        <button className={`tab ${tab === "overview" ? "active" : ""}`} role="tab" aria-selected={tab === "overview"} onClick={() => setTab("overview")}>
          Overview
        </button>
        <button className={`tab ${tab === "logs" ? "active" : ""}`} role="tab" aria-selected={tab === "logs"} onClick={() => setTab("logs")}>
          Logs
        </button>
        <button className={`tab ${tab === "placement" ? "active" : ""}`} role="tab" aria-selected={tab === "placement"} onClick={() => setTab("placement")}>
          Placement
        </button>
      </div>

      {tab === "overview" && (
        <>
          <div className="section">
            <div className="section-title">Status</div>
            <dl className="kv-grid">
              <dt>Node</dt>
              <dd>{node ? <Link to={`/nodes/${encodeURIComponent(node.name)}`}>{node.name}</Link> : "unscheduled"}</dd>
              <dt>Container ID</dt>
              <dd className="mono">{w.status.containerId || "—"}</dd>
              <dt>Restarts</dt>
              <dd>{w.status.restartCount}</dd>
              <dt>Exit code</dt>
              <dd>{w.status.exitCode ?? "—"}</dd>
              <dt>Message</dt>
              <dd>{w.status.message || "—"}</dd>
              <dt>Reason</dt>
              <dd>{w.status.reason || "—"}</dd>
              <dt>Started</dt>
              <dd>{formatDateTime(w.status.startedAt)}</dd>
              <dt>Finished</dt>
              <dd>{formatDateTime(w.status.finishedAt)}</dd>
              <dt>CPU / Memory used</dt>
              <dd>{formatResources(w.status.usage)}</dd>
              <dt>Owner</dt>
              <dd>
                {w.ownerRef ? (
                  <Link to={`/deployments/${encodeURIComponent(w.ownerRef.name)}`}>
                    {w.ownerRef.kind} {w.ownerRef.name}
                  </Link>
                ) : (
                  "none"
                )}
              </dd>
              <dt>Created</dt>
              <dd>{formatDateTime(w.createdAt)}</dd>
            </dl>
          </div>

          <div className="section">
            <div className="section-title">Spec</div>
            <dl className="kv-grid">
              <dt>Command</dt>
              <dd className="mono">{w.spec.command?.join(" ") || "(image default)"}</dd>
              <dt>Args</dt>
              <dd className="mono">{w.spec.args?.join(" ") || "—"}</dd>
              <dt>Resources (request / limit)</dt>
              <dd>
                {formatResources(w.spec.resources.request)}
                {w.spec.resources.limit ? ` / ${formatResources(w.spec.resources.limit)}` : ""}
              </dd>
              <dt>Restart policy</dt>
              <dd>{w.spec.restartPolicy}</dd>
              <dt>Termination grace period</dt>
              <dd>{formatGoDuration(w.spec.terminationGracePeriod)}</dd>
              <dt>Priority</dt>
              <dd>{w.spec.priority}</dd>
              <dt>Ports</dt>
              <dd>
                {w.spec.ports && w.spec.ports.length > 0
                  ? w.spec.ports
                      .map(
                        (p) =>
                          `${p.container}/${p.protocol ?? "tcp"}${p.host ? ` → host ${p.host}` : ""}`,
                      )
                      .join(", ")
                  : "none"}
              </dd>
              <dt>Env</dt>
              <dd>
                {w.spec.env && w.spec.env.length > 0
                  ? w.spec.env.map((e) => `${e.name}=${e.value}`).join(", ")
                  : "none"}
              </dd>
              <dt>Node selector</dt>
              <dd>
                {w.spec.nodeSelector && Object.keys(w.spec.nodeSelector).length > 0
                  ? Object.entries(w.spec.nodeSelector).map(([k, v]) => `${k}=${v}`).join(", ")
                  : "none"}
              </dd>
              <dt>Health check</dt>
              <dd>
                {w.spec.healthCheck
                  ? `${w.spec.healthCheck.kind}${w.spec.healthCheck.path ? ` ${w.spec.healthCheck.path}` : ""}${
                      w.spec.healthCheck.port ? `:${w.spec.healthCheck.port}` : ""
                    } every ${formatGoDuration(w.spec.healthCheck.interval)}`
                  : "process (default)"}
              </dd>
            </dl>
          </div>

          <div className="section">
            <div className="section-title">Recent events</div>
            {events.length === 0 ? (
              <p className="text-muted">No events recorded for this workload.</p>
            ) : (
              <div className="table-wrap">
                <table className="data-table">
                  <thead>
                    <tr>
                      <th>Time</th>
                      <th>Severity</th>
                      <th>Reason</th>
                      <th className="wrap">Message</th>
                    </tr>
                  </thead>
                  <tbody>
                    {events.map((e) => (
                      <tr key={e.id}>
                        <td>{formatDateTime(e.timestamp)}</td>
                        <td>
                          <StatusBadge label={e.severity} tone={severityTone(e.severity)} />
                        </td>
                        <td>{e.reason}</td>
                        <td className="wrap">{e.message}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        </>
      )}

      {tab === "logs" && (
        <div className="section">
          <LogViewer workloadName={w.name} />
        </div>
      )}

      {tab === "placement" && (
        <div className="section">
          {!w.status.placement ? (
            <p className="text-muted">This workload has not been scheduled yet.</p>
          ) : (
            <>
              <dl className="kv-grid">
                <dt>Decided at</dt>
                <dd>{formatDateTime(w.status.placement.decidedAt)}</dd>
                <dt>Winning node</dt>
                <dd>{w.status.placement.nodeName || "(unscheduled)"}</dd>
                <dt>Score</dt>
                <dd>{w.status.placement.score}</dd>
                <dt>Reason</dt>
                <dd>{w.status.placement.reason}</dd>
                <dt>Scheduling latency</dt>
                <dd>{w.status.placement.latencyMicros} µs</dd>
              </dl>

              {w.status.placement.candidates && w.status.placement.candidates.length > 0 && (
                <div style={{ marginTop: 16 }}>
                  <div className="section-title">Candidates</div>
                  <div className="table-wrap">
                    <table className="data-table">
                      <thead>
                        <tr>
                          <th>Node</th>
                          <th className="num">Score</th>
                          <th className="wrap">Breakdown</th>
                        </tr>
                      </thead>
                      <tbody>
                        {w.status.placement.candidates.map((c) => (
                          <tr key={c.nodeName}>
                            <td>{c.nodeName}</td>
                            <td className="num">{c.score}</td>
                            <td className="wrap mono">
                              {c.breakdown
                                ? Object.entries(c.breakdown).map(([k, v]) => `${k}=${v}`).join(" ")
                                : "—"}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}

              {w.status.placement.rejections && w.status.placement.rejections.length > 0 && (
                <div style={{ marginTop: 16 }}>
                  <div className="section-title">Rejected nodes</div>
                  <div className="table-wrap">
                    <table className="data-table">
                      <thead>
                        <tr>
                          <th>Node</th>
                          <th>Filter</th>
                          <th className="wrap">Reason</th>
                        </tr>
                      </thead>
                      <tbody>
                        {w.status.placement.rejections.map((rj) => (
                          <tr key={rj.nodeName}>
                            <td>{rj.nodeName}</td>
                            <td>{rj.filter}</td>
                            <td className="wrap">{rj.reason}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}
            </>
          )}
        </div>
      )}

      {confirmDelete && (
        <ConfirmDialog
          title={`Delete ${w.name}?`}
          danger
          confirmLabel="Delete workload"
          busy={busy}
          onConfirm={doDelete}
          onCancel={() => {
            setConfirmDelete(false);
            setForce(false);
          }}
          body={
            <div className="form-grid">
              <p>This permanently terminates the container and removes the workload record.</p>
              {w.ownerRef && (
                <>
                  <p className="text-critical">
                    {w.name} is managed by {w.ownerRef.kind} "{w.ownerRef.name}" and will be recreated unless you
                    force this delete.
                  </p>
                  <label className="checkbox-row">
                    <input type="checkbox" checked={force} onChange={(e) => setForce(e.target.checked)} />
                    Force delete anyway (?force=true)
                  </label>
                </>
              )}
            </div>
          }
        />
      )}
    </div>
  );
}
