import { getCluster } from "../api/resources";
import { useAsync } from "../hooks/useAsync";
import { ErrorState, LoadingState } from "../components/States";
import { StatusBadge } from "../components/StatusBadge";
import { formatDateTime } from "../utils/format";
import { memberRoleTone } from "../utils/status";

export function Clusters() {
  const cluster = useAsync(() => getCluster(), []);

  if (cluster.initialLoading) return <div className="page"><LoadingState label="Loading cluster…" /></div>;
  if (cluster.error) return <div className="page"><ErrorState error={cluster.error} onRetry={cluster.refetch} /></div>;
  if (!cluster.data) return null;

  const { cluster: c } = cluster.data;

  return (
    <div className="page">
      <div className="page-header">
        <div>
          <h1>Clusters</h1>
          <p className="page-subtitle">
            {c.name} · id {c.id} · orion {c.version} · created {formatDateTime(c.createdAt)}
          </p>
        </div>
        <div className="page-actions">
          <button className="btn" onClick={cluster.refetch}>
            Refresh
          </button>
        </div>
      </div>

      <div className="section">
        <div className="section-title">Consensus</div>
        <div className="stat-grid">
          <div className="stat-cell">
            <div className="stat-label">Quorum</div>
            <div className={`stat-value ${c.quorumHealthy ? "tone-healthy" : "tone-critical"}`}>
              {c.quorumHealthy ? "Healthy" : "Degraded"}
            </div>
            <div className="stat-sub">requires {c.quorum}</div>
          </div>
          <div className="stat-cell">
            <div className="stat-label">Leader</div>
            <div className="stat-value" style={{ fontSize: 14 }}>
              {c.leaderId || "none"}
            </div>
          </div>
          <div className="stat-cell">
            <div className="stat-label">Raft term</div>
            <div className="stat-value">{c.raftTerm}</div>
          </div>
          <div className="stat-cell">
            <div className="stat-label">Commit index</div>
            <div className="stat-value">{c.commitIndex}</div>
          </div>
          <div className="stat-cell">
            <div className="stat-label">Applied index</div>
            <div className="stat-value">{c.appliedIndex}</div>
          </div>
        </div>
      </div>

      <div className="section">
        <div className="section-title">Control plane members</div>
        <div className="table-wrap">
          <table className="data-table">
            <thead>
              <tr>
                <th>ID</th>
                <th>Address</th>
                <th>Role</th>
                <th>Reachable</th>
              </tr>
            </thead>
            <tbody>
              {c.controlPlane.length === 0 && (
                <tr>
                  <td colSpan={4} className="text-muted">
                    No control-plane members reported.
                  </td>
                </tr>
              )}
              {c.controlPlane.map((m) => (
                <tr key={m.id}>
                  <td className="mono">{m.id}</td>
                  <td className="mono">{m.address}</td>
                  <td>
                    <StatusBadge label={m.role} tone={memberRoleTone(m.role)} />
                  </td>
                  <td>
                    <StatusBadge
                      label={m.reachable ? "Reachable" : "Unreachable"}
                      tone={m.reachable ? "healthy" : "critical"}
                    />
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
