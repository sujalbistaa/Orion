import { useState } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";
import {
  deleteDeployment,
  getDeployment,
  rollbackDeployment,
  scaleDeployment,
} from "../api/resources";
import { useAsync } from "../hooks/useAsync";
import { useWatchRefetch } from "../api/WatchProvider";
import { ErrorState, LoadingState } from "../components/States";
import { StatusBadge } from "../components/StatusBadge";
import { ConfirmDialog } from "../components/ConfirmDialog";
import { useToast, describeError } from "../components/Toast";
import { formatAge, formatDateTime, formatResources } from "../utils/format";
import { deploymentPhaseTone, healthTone, severityTone, workloadPhaseTone } from "../utils/status";

export function DeploymentDetail() {
  const { name = "" } = useParams();
  const navigate = useNavigate();
  const toast = useToast();
  const detail = useAsync(() => getDeployment(name), [name]);
  useWatchRefetch(["Deployment", "Workload"], detail.refetch);

  const [scaleValue, setScaleValue] = useState<string | null>(null);
  const [confirmDelete, setConfirmDelete] = useState(false);
  const [rollbackTarget, setRollbackTarget] = useState<number | null>(null);
  const [busy, setBusy] = useState(false);

  if (detail.initialLoading) return <div className="page"><LoadingState label="Loading deployment…" /></div>;
  if (detail.error) return <div className="page"><ErrorState error={detail.error} onRetry={detail.refetch} /></div>;
  if (!detail.data) return null;

  const { deployment: d, workloads, revisions, events } = detail.data;

  async function doScale() {
    if (scaleValue === null) return;
    setBusy(true);
    try {
      await scaleDeployment(d.name, Number(scaleValue));
      toast.push("success", `Scaled to ${scaleValue} replicas`);
      setScaleValue(null);
      detail.refetch();
    } catch (err) {
      toast.push("error", describeError(err));
    } finally {
      setBusy(false);
    }
  }

  async function doRollback() {
    if (rollbackTarget === null) return;
    setBusy(true);
    try {
      await rollbackDeployment(d.name, rollbackTarget);
      toast.push("success", `Rolled back to revision ${rollbackTarget}`);
      setRollbackTarget(null);
      detail.refetch();
    } catch (err) {
      toast.push("error", describeError(err));
    } finally {
      setBusy(false);
    }
  }

  async function doDelete() {
    setBusy(true);
    try {
      await deleteDeployment(d.name);
      toast.push("success", `${d.name} deleted`);
      navigate("/deployments");
    } catch (err) {
      toast.push("error", describeError(err));
      setBusy(false);
    }
  }

  return (
    <div className="page">
      <div className="breadcrumb">
        <Link to="/deployments">Deployments</Link> / {d.name}
      </div>
      <div className="page-header">
        <div>
          <h1>{d.name}</h1>
          <p className="page-subtitle">
            <StatusBadge label={d.status.phase} tone={deploymentPhaseTone(d.status.phase)} />{" "}
            <span className="mono">{d.spec.template.image}</span> · revision {d.status.revision}
          </p>
        </div>
        <div className="page-actions">
          <button className="btn" onClick={() => setScaleValue(String(d.spec.replicas))}>
            Scale
          </button>
          <button className="btn btn-danger" onClick={() => setConfirmDelete(true)}>
            Delete
          </button>
        </div>
      </div>

      <div className="section">
        <div className="section-title">Rollout status</div>
        <div className="stat-grid">
          <div className="stat-cell">
            <div className="stat-label">Desired</div>
            <div className="stat-value">{d.status.desiredReplicas}</div>
          </div>
          <div className="stat-cell">
            <div className="stat-label">Current</div>
            <div className="stat-value">{d.status.currentReplicas}</div>
          </div>
          <div className="stat-cell">
            <div className="stat-label">Updated</div>
            <div className="stat-value">{d.status.updatedReplicas}</div>
          </div>
          <div className="stat-cell">
            <div className="stat-label">Available</div>
            <div className="stat-value tone-healthy">{d.status.availableReplicas}</div>
          </div>
          <div className="stat-cell">
            <div className="stat-label">Unschedulable</div>
            <div className={`stat-value ${d.status.unschedulableReplicas > 0 ? "tone-warning" : ""}`}>
              {d.status.unschedulableReplicas}
            </div>
          </div>
        </div>
      </div>

      <div className="section">
        <div className="section-title">Spec</div>
        <dl className="kv-grid">
          <dt>Strategy</dt>
          <dd>
            {d.spec.strategy.kind}
            {d.spec.strategy.kind === "RollingUpdate" &&
              ` (maxUnavailable=${d.spec.strategy.maxUnavailable}, maxSurge=${d.spec.strategy.maxSurge})`}
          </dd>
          <dt>Resources (request)</dt>
          <dd>{formatResources(d.spec.template.resources.request)}</dd>
          <dt>Restart policy</dt>
          <dd>{d.spec.template.restartPolicy}</dd>
          <dt>Created</dt>
          <dd>{formatDateTime(d.createdAt)}</dd>
        </dl>
      </div>

      <div className="section">
        <div className="section-title">Replicas ({workloads.length})</div>
        {workloads.length === 0 ? (
          <p className="text-muted">No replicas yet.</p>
        ) : (
          <div className="table-wrap">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Name</th>
                  <th>Phase</th>
                  <th>Health</th>
                  <th>Node</th>
                  <th>Age</th>
                </tr>
              </thead>
              <tbody>
                {workloads.map((w) => (
                  <tr key={w.uid}>
                    <td>
                      <Link to={`/workloads/${encodeURIComponent(w.name)}`}>{w.name}</Link>
                    </td>
                    <td>
                      <StatusBadge label={w.status.phase} tone={workloadPhaseTone(w.status.phase)} />
                    </td>
                    <td>
                      <StatusBadge label={w.status.health} tone={healthTone(w.status.health)} />
                    </td>
                    <td>
                      {w.status.nodeName ? (
                        <Link to={`/nodes/${encodeURIComponent(w.status.nodeName)}`}>{w.status.nodeName}</Link>
                      ) : (
                        "—"
                      )}
                    </td>
                    <td>{formatAge(w.createdAt)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      <div className="section">
        <div className="section-title">Revision history</div>
        {revisions.length === 0 ? (
          <p className="text-muted">No rollout history recorded.</p>
        ) : (
          <div className="table-wrap">
            <table className="data-table">
              <thead>
                <tr>
                  <th className="num">Revision</th>
                  <th>Image</th>
                  <th className="num">Replicas</th>
                  <th>Created</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody>
                {revisions.map((r) => (
                  <tr key={r.revision}>
                    <td className="num">
                      {r.revision} {r.revision === d.status.revision && <StatusBadge label="current" tone="info" />}
                    </td>
                    <td className="mono">{r.template.image}</td>
                    <td className="num">{r.replicas}</td>
                    <td>{formatDateTime(r.createdAt)}</td>
                    <td>
                      {r.revision !== d.status.revision && (
                        <button className="btn btn-sm" onClick={() => setRollbackTarget(r.revision)}>
                          Rollback to this
                        </button>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      <div className="section">
        <div className="section-title">Recent events</div>
        {events.length === 0 ? (
          <p className="text-muted">No events recorded for this deployment.</p>
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

      {scaleValue !== null && (
        <ConfirmDialog
          title={`Scale ${d.name}`}
          confirmLabel="Scale"
          busy={busy}
          onConfirm={doScale}
          onCancel={() => setScaleValue(null)}
          body={
            <div className="field">
              <label htmlFor="scale-input">Replicas</label>
              <input
                id="scale-input"
                type="number"
                min="0"
                max="1000"
                value={scaleValue}
                onChange={(e) => setScaleValue(e.target.value)}
                autoFocus
              />
              <span className="hint">Currently {d.spec.replicas} desired, {d.status.availableReplicas} available.</span>
            </div>
          }
        />
      )}

      {rollbackTarget !== null && (
        <ConfirmDialog
          title={`Rollback ${d.name} to revision ${rollbackTarget}?`}
          confirmLabel="Rollback"
          busy={busy}
          onConfirm={doRollback}
          onCancel={() => setRollbackTarget(null)}
          body={<p>This re-applies the template recorded at revision {rollbackTarget} and rolls out replacement replicas.</p>}
        />
      )}

      {confirmDelete && (
        <ConfirmDialog
          title={`Delete ${d.name}?`}
          danger
          confirmLabel="Delete deployment"
          busy={busy}
          onConfirm={doDelete}
          onCancel={() => setConfirmDelete(false)}
          body={<p>This deletes the deployment and its {d.status.currentReplicas} replica(s).</p>}
        />
      )}
    </div>
  );
}
