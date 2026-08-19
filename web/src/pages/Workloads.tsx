import { useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { deleteWorkload, listWorkloads } from "../api/resources";
import { useAsync } from "../hooks/useAsync";
import { useWatchRefetch } from "../api/WatchProvider";
import { ErrorState, LoadingState, EmptyState } from "../components/States";
import { StatusBadge } from "../components/StatusBadge";
import { ConfirmDialog } from "../components/ConfirmDialog";
import { useToast, describeError } from "../components/Toast";
import { formatAge, formatBytes, formatCPU } from "../utils/format";
import { healthTone, workloadPhaseTone } from "../utils/status";
import type { Workload } from "../api/types";

const PHASES = ["Pending", "Scheduled", "Starting", "Running", "Succeeded", "Failed", "Terminating", "Terminated"];

export function Workloads() {
  const workloads = useAsync(() => listWorkloads(), []);
  useWatchRefetch(["Workload"], workloads.refetch);
  const toast = useToast();

  const [phaseFilter, setPhaseFilter] = useState("");
  const [search, setSearch] = useState("");
  const [target, setTarget] = useState<Workload | null>(null);
  const [force, setForce] = useState(false);
  const [busy, setBusy] = useState(false);

  const filtered = useMemo(() => {
    let items = workloads.data?.items ?? [];
    if (phaseFilter) items = items.filter((w) => w.status.phase === phaseFilter);
    if (search.trim()) {
      const q = search.trim().toLowerCase();
      items = items.filter((w) => w.name.toLowerCase().includes(q) || w.spec.image.toLowerCase().includes(q));
    }
    return items;
  }, [workloads.data, phaseFilter, search]);

  async function doDelete() {
    if (!target) return;
    setBusy(true);
    try {
      await deleteWorkload(target.name, { force });
      toast.push("success", `${target.name} deleted`);
      setTarget(null);
      setForce(false);
      workloads.refetch();
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
          <h1>Workloads</h1>
          <p className="page-subtitle">Single-container schedulable units running on the cluster.</p>
        </div>
        <div className="page-actions">
          <button className="btn" onClick={workloads.refetch}>
            Refresh
          </button>
        </div>
      </div>

      <div className="toolbar">
        <input
          className="filter-input"
          type="search"
          placeholder="Filter by name or image…"
          aria-label="Filter workloads"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
        />
        <select
          className="filter-select"
          aria-label="Filter by phase"
          value={phaseFilter}
          onChange={(e) => setPhaseFilter(e.target.value)}
        >
          <option value="">All phases</option>
          {PHASES.map((p) => (
            <option key={p} value={p}>
              {p}
            </option>
          ))}
        </select>
        <span className="spacer" />
        <span className="text-faint">{filtered.length} shown</span>
      </div>

      {workloads.error && <ErrorState error={workloads.error} onRetry={workloads.refetch} />}
      {!workloads.error && workloads.initialLoading && <LoadingState label="Loading workloads…" />}
      {!workloads.error && !workloads.initialLoading && (workloads.data?.items.length ?? 0) === 0 && (
        <EmptyState title="No workloads" detail="Create a deployment, or submit a workload directly via the API." />
      )}
      {!workloads.error && !workloads.initialLoading && (workloads.data?.items.length ?? 0) > 0 && (
        <div className="table-wrap">
          <table className="data-table">
            <thead>
              <tr>
                <th>Name</th>
                <th>Phase</th>
                <th>Health</th>
                <th>Node</th>
                <th className="num">CPU used</th>
                <th className="num">Mem used</th>
                <th className="num">Restarts</th>
                <th>Age</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {filtered.length === 0 && (
                <tr>
                  <td colSpan={9} className="text-muted">
                    No workloads match this filter.
                  </td>
                </tr>
              )}
              {filtered.map((w) => (
                <tr key={w.uid}>
                  <td>
                    <Link to={`/workloads/${encodeURIComponent(w.name)}`}>{w.name}</Link>
                    {w.ownerRef && <span className="text-faint"> · owned by {w.ownerRef.name}</span>}
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
                  <td className="num">{formatCPU(w.status.usage.cpu)}</td>
                  <td className="num">{formatBytes(w.status.usage.memory)}</td>
                  <td className="num">{w.status.restartCount}</td>
                  <td>{formatAge(w.createdAt)}</td>
                  <td>
                    <div style={{ display: "flex", gap: 6 }}>
                      <Link className="btn btn-sm" to={`/workloads/${encodeURIComponent(w.name)}`}>
                        Details
                      </Link>
                      <button className="btn btn-sm btn-danger" onClick={() => setTarget(w)}>
                        Delete
                      </button>
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {target && (
        <ConfirmDialog
          title={`Delete ${target.name}?`}
          danger
          confirmLabel="Delete workload"
          busy={busy}
          onConfirm={doDelete}
          onCancel={() => {
            setTarget(null);
            setForce(false);
          }}
          body={
            <div className="form-grid">
              <p>This permanently terminates the container and removes the workload record.</p>
              {target.ownerRef && (
                <>
                  <p className="text-critical">
                    {target.name} is managed by {target.ownerRef.kind} "{target.ownerRef.name}" and will be
                    recreated unless you force this delete. Scale or delete the {target.ownerRef.kind.toLowerCase()}{" "}
                    instead if that's what you mean.
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
