import { useState } from "react";
import { Link } from "react-router-dom";
import { cordonNode, drainNode, listNodes, uncordonNode } from "../api/resources";
import { useAsync } from "../hooks/useAsync";
import { useWatchRefetch } from "../api/WatchProvider";
import { ErrorState, LoadingState, EmptyState } from "../components/States";
import { StatusBadge } from "../components/StatusBadge";
import { ConfirmDialog } from "../components/ConfirmDialog";
import { useToast, describeError } from "../components/Toast";
import { formatAge, formatBytes, formatCPU, formatDateTime } from "../utils/format";
import { nodePhaseTone } from "../utils/status";
import type { Node } from "../api/types";

type PendingAction =
  | { kind: "drain"; node: Node }
  | { kind: "cordon" | "uncordon"; node: Node };

export function Nodes() {
  const nodes = useAsync(() => listNodes(), []);
  useWatchRefetch(["Node"], nodes.refetch);
  const toast = useToast();
  const [pending, setPending] = useState<PendingAction | null>(null);
  const [busy, setBusy] = useState(false);
  const [rowBusy, setRowBusy] = useState<string | null>(null);

  async function runToggleCordon(node: Node) {
    setRowBusy(node.name);
    try {
      if (node.spec.unschedulable) {
        await uncordonNode(node.name);
        toast.push("success", `${node.name} uncordoned`);
      } else {
        await cordonNode(node.name);
        toast.push("success", `${node.name} cordoned`);
      }
      nodes.refetch();
    } catch (err) {
      toast.push("error", describeError(err));
    } finally {
      setRowBusy(null);
    }
  }

  async function confirmDrain() {
    if (!pending || pending.kind !== "drain") return;
    setBusy(true);
    try {
      await drainNode(pending.node.name, false);
      toast.push("success", `${pending.node.name} is draining`);
      nodes.refetch();
      setPending(null);
    } catch (err) {
      toast.push("error", describeError(err));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="page">
      <div className="page-header">
        <div>
          <h1>Nodes</h1>
          <p className="page-subtitle">Machines registered with the control plane.</p>
        </div>
        <div className="page-actions">
          <button className="btn" onClick={nodes.refetch}>
            Refresh
          </button>
        </div>
      </div>

      {nodes.error && <ErrorState error={nodes.error} onRetry={nodes.refetch} />}
      {!nodes.error && nodes.initialLoading && <LoadingState label="Loading nodes…" />}
      {!nodes.error && !nodes.initialLoading && (nodes.data?.items.length ?? 0) === 0 && (
        <EmptyState title="No nodes registered" detail="Start an orion-agent to register a node with this cluster." />
      )}
      {!nodes.error && !nodes.initialLoading && (nodes.data?.items.length ?? 0) > 0 && (
        <div className="table-wrap">
          <table className="data-table">
            <thead>
              <tr>
                <th>Name</th>
                <th>Phase</th>
                <th>Schedulable</th>
                <th className="num">CPU alloc / allocatable</th>
                <th className="num">Mem alloc / allocatable</th>
                <th className="num">Workloads</th>
                <th>Runtime</th>
                <th>Last heartbeat</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {nodes.data!.items.map((n) => (
                <tr key={n.uid}>
                  <td>
                    <Link to={`/nodes/${encodeURIComponent(n.name)}`}>{n.name}</Link>
                  </td>
                  <td>
                    <StatusBadge label={n.status.phase} tone={nodePhaseTone(n.status.phase)} />
                  </td>
                  <td>
                    {n.spec.unschedulable ? (
                      <StatusBadge label="Cordoned" tone="warning" />
                    ) : (
                      <StatusBadge label="Schedulable" tone="healthy" />
                    )}
                  </td>
                  <td className="num">
                    {formatCPU(n.status.allocated.cpu)} / {formatCPU(n.status.allocatable.cpu)}
                  </td>
                  <td className="num">
                    {formatBytes(n.status.allocated.memory)} / {formatBytes(n.status.allocatable.memory)}
                  </td>
                  <td className="num">{n.status.workloadCount}</td>
                  <td>{n.status.runtime.name || "—"}</td>
                  <td title={formatDateTime(n.status.lastHeartbeat)}>{formatAge(n.status.lastHeartbeat)} ago</td>
                  <td>
                    <div style={{ display: "flex", gap: 6 }}>
                      <button
                        className="btn btn-sm"
                        disabled={rowBusy === n.name}
                        onClick={() => runToggleCordon(n)}
                      >
                        {n.spec.unschedulable ? "Uncordon" : "Cordon"}
                      </button>
                      <button
                        className="btn btn-sm"
                        disabled={n.status.phase === "Draining" || n.status.phase === "Decommissioned"}
                        onClick={() => setPending({ kind: "drain", node: n })}
                      >
                        Drain
                      </button>
                      <Link className="btn btn-sm" to={`/nodes/${encodeURIComponent(n.name)}`}>
                        Details
                      </Link>
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {pending?.kind === "drain" && (
        <ConfirmDialog
          title={`Drain ${pending.node.name}?`}
          body={
            <p>
              This stops new scheduling on <strong>{pending.node.name}</strong> and evicts its workloads in a
              controlled fashion. If this is the only Ready node with running workloads, the API will refuse the
              request.
            </p>
          }
          confirmLabel="Drain node"
          busy={busy}
          onConfirm={confirmDrain}
          onCancel={() => setPending(null)}
        />
      )}
    </div>
  );
}
