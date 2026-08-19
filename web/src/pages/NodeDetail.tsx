import { useState } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";
import {
  cordonNode,
  deleteNode,
  drainNode,
  getNode,
  uncordonNode,
} from "../api/resources";
import { useAsync } from "../hooks/useAsync";
import { useWatchRefetch } from "../api/WatchProvider";
import { ErrorState, LoadingState } from "../components/States";
import { StatusBadge } from "../components/StatusBadge";
import { ConfirmDialog } from "../components/ConfirmDialog";
import { useToast, describeError } from "../components/Toast";
import { formatAge, formatBytes, formatCPU, formatDateTime } from "../utils/format";
import { nodePhaseTone, severityTone, workloadPhaseTone, healthTone } from "../utils/status";

export function NodeDetail() {
  const { name = "" } = useParams();
  const navigate = useNavigate();
  const toast = useToast();
  const detail = useAsync(() => getNode(name), [name]);
  useWatchRefetch(["Node", "Workload"], detail.refetch);

  const [confirmDelete, setConfirmDelete] = useState(false);
  const [confirmDrain, setConfirmDrain] = useState(false);
  const [forceDelete, setForceDelete] = useState(false);
  const [busy, setBusy] = useState(false);

  if (detail.initialLoading) return <div className="page"><LoadingState label="Loading node…" /></div>;
  if (detail.error) return <div className="page"><ErrorState error={detail.error} onRetry={detail.refetch} /></div>;
  if (!detail.data) return null;

  const { node, workloads, events } = detail.data;

  async function toggleCordon() {
    setBusy(true);
    try {
      if (node.spec.unschedulable) {
        await uncordonNode(node.name);
        toast.push("success", "Node uncordoned");
      } else {
        await cordonNode(node.name);
        toast.push("success", "Node cordoned");
      }
      detail.refetch();
    } catch (err) {
      toast.push("error", describeError(err));
    } finally {
      setBusy(false);
    }
  }

  async function doDrain() {
    setBusy(true);
    try {
      await drainNode(node.name, false);
      toast.push("success", "Node is draining");
      setConfirmDrain(false);
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
      await deleteNode(node.name, forceDelete);
      toast.push("success", `${node.name} deleted`);
      navigate("/nodes");
    } catch (err) {
      toast.push("error", describeError(err));
      setBusy(false);
    }
  }

  return (
    <div className="page">
      <div className="breadcrumb">
        <Link to="/nodes">Nodes</Link> / {node.name}
      </div>
      <div className="page-header">
        <div>
          <h1>{node.name}</h1>
          <p className="page-subtitle">
            <StatusBadge label={node.status.phase} tone={nodePhaseTone(node.status.phase)} />{" "}
            {node.spec.unschedulable && <StatusBadge label="Cordoned" tone="warning" />} uid {node.uid}
          </p>
        </div>
        <div className="page-actions">
          <button className="btn" onClick={toggleCordon} disabled={busy}>
            {node.spec.unschedulable ? "Uncordon" : "Cordon"}
          </button>
          <button
            className="btn"
            onClick={() => setConfirmDrain(true)}
            disabled={busy || node.status.phase === "Draining" || node.status.phase === "Decommissioned"}
          >
            Drain
          </button>
          <button className="btn btn-danger" onClick={() => setConfirmDelete(true)} disabled={busy}>
            Delete
          </button>
        </div>
      </div>

      <div className="section">
        <div className="section-title">Resources</div>
        <div className="stat-grid">
          <div className="stat-cell">
            <div className="stat-label">CPU capacity</div>
            <div className="stat-value">{formatCPU(node.status.capacity.cpu)}</div>
          </div>
          <div className="stat-cell">
            <div className="stat-label">CPU allocatable</div>
            <div className="stat-value">{formatCPU(node.status.allocatable.cpu)}</div>
          </div>
          <div className="stat-cell">
            <div className="stat-label">CPU allocated</div>
            <div className="stat-value">{formatCPU(node.status.allocated.cpu)}</div>
          </div>
          <div className="stat-cell">
            <div className="stat-label">CPU used</div>
            <div className="stat-value">{formatCPU(node.status.usage.cpu)}</div>
          </div>
          <div className="stat-cell">
            <div className="stat-label">Memory capacity</div>
            <div className="stat-value">{formatBytes(node.status.capacity.memory)}</div>
          </div>
          <div className="stat-cell">
            <div className="stat-label">Memory allocatable</div>
            <div className="stat-value">{formatBytes(node.status.allocatable.memory)}</div>
          </div>
          <div className="stat-cell">
            <div className="stat-label">Memory allocated</div>
            <div className="stat-value">{formatBytes(node.status.allocated.memory)}</div>
          </div>
          <div className="stat-cell">
            <div className="stat-label">Memory used</div>
            <div className="stat-value">{formatBytes(node.status.usage.memory)}</div>
          </div>
        </div>
      </div>

      <div className="section">
        <div className="section-title">Node info</div>
        <dl className="kv-grid">
          <dt>Address</dt>
          <dd className="mono">{node.spec.address}</dd>
          <dt>Runtime</dt>
          <dd>
            {node.status.runtime.name} {node.status.runtime.version} ({node.status.runtime.os}/
            {node.status.runtime.arch})
          </dd>
          <dt>Last heartbeat</dt>
          <dd>{formatDateTime(node.status.lastHeartbeat)}</dd>
          <dt>Agent started</dt>
          <dd>{formatDateTime(node.status.agentStartedAt)}</dd>
          <dt>Created</dt>
          <dd>{formatDateTime(node.createdAt)}</dd>
          <dt>Taints</dt>
          <dd>
            {node.spec.taints && node.spec.taints.length > 0
              ? node.spec.taints.map((t) => `${t.key}${t.value ? `=${t.value}` : ""}:${t.effect}`).join(", ")
              : "none"}
          </dd>
        </dl>
      </div>

      {node.status.conditions && node.status.conditions.length > 0 && (
        <div className="section">
          <div className="section-title">Conditions</div>
          <div className="table-wrap">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Type</th>
                  <th>Status</th>
                  <th>Reason</th>
                  <th className="wrap">Message</th>
                  <th>Since</th>
                </tr>
              </thead>
              <tbody>
                {node.status.conditions.map((c, i) => (
                  <tr key={i}>
                    <td>{c.type}</td>
                    <td>
                      <StatusBadge label={c.status ? "True" : "False"} tone={c.status ? "healthy" : "critical"} />
                    </td>
                    <td>{c.reason}</td>
                    <td className="wrap">{c.message}</td>
                    <td>{formatDateTime(c.since)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      <div className="section">
        <div className="section-title">Workloads on this node ({workloads.length})</div>
        {workloads.length === 0 ? (
          <p className="text-muted">No workloads are scheduled on this node.</p>
        ) : (
          <div className="table-wrap">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Name</th>
                  <th>Phase</th>
                  <th>Health</th>
                  <th className="num">CPU used</th>
                  <th className="num">Mem used</th>
                  <th className="num">Restarts</th>
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
                    <td className="num">{formatCPU(w.status.usage.cpu)}</td>
                    <td className="num">{formatBytes(w.status.usage.memory)}</td>
                    <td className="num">{w.status.restartCount}</td>
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
          <p className="text-muted">No events recorded for this node.</p>
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
                    <td title={formatDateTime(e.timestamp)}>{formatAge(e.timestamp)} ago</td>
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

      {confirmDrain && (
        <ConfirmDialog
          title={`Drain ${node.name}?`}
          body={<p>New scheduling stops immediately and existing workloads are evicted in a controlled fashion.</p>}
          confirmLabel="Drain node"
          busy={busy}
          onConfirm={doDrain}
          onCancel={() => setConfirmDrain(false)}
        />
      )}

      {confirmDelete && (
        <ConfirmDialog
          title={`Delete ${node.name}?`}
          danger
          confirmLabel="Delete node"
          busy={busy}
          onConfirm={doDelete}
          onCancel={() => setConfirmDelete(false)}
          body={
            <div className="form-grid">
              <p>
                This permanently removes the node record. {node.status.phase === "Ready" && (
                  <>The node is currently <strong>Ready</strong> and may still be running workloads; deleting it
                  without force will be refused by the API.</>
                )}
              </p>
              <label className="checkbox-row">
                <input
                  type="checkbox"
                  checked={forceDelete}
                  onChange={(e) => setForceDelete(e.target.checked)}
                />
                Force delete (terminate any workloads still running here)
              </label>
            </div>
          }
        />
      )}
    </div>
  );
}
