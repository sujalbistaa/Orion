import { Link } from "react-router-dom";
import { getCluster, listEvents } from "../api/resources";
import { useAsync } from "../hooks/useAsync";
import { useWatchRefetch } from "../api/WatchProvider";
import { ErrorState, LoadingState } from "../components/States";
import { StatusBadge } from "../components/StatusBadge";
import { formatBytes, formatCPU, formatDateTime, formatPercent } from "../utils/format";
import { severityTone } from "../utils/status";

export function Overview() {
  const cluster = useAsync(() => getCluster(), []);
  const events = useAsync(() => listEvents({ limit: 15 }), []);

  useWatchRefetch(["Node", "Workload", "Deployment", "Service"], cluster.refetch);
  useWatchRefetch(["Event"], events.refetch);

  if (cluster.initialLoading) return <div className="page"><LoadingState label="Loading cluster summary…" /></div>;
  if (cluster.error) return <div className="page"><ErrorState error={cluster.error} onRetry={cluster.refetch} /></div>;
  if (!cluster.data) return null;

  const { cluster: c, summary: s } = cluster.data;

  return (
    <div className="page">
      <div className="page-header">
        <div>
          <h1>Overview</h1>
          <p className="page-subtitle">
            {c.name} · applied index {s.appliedIndex} · {c.quorumHealthy ? "quorum healthy" : "quorum degraded"}
          </p>
        </div>
      </div>

      <div className="section">
        <div className="section-title">Nodes</div>
        <div className="stat-grid">
          <div className="stat-cell">
            <div className="stat-label">Total</div>
            <div className="stat-value">{s.nodes.total}</div>
          </div>
          <div className="stat-cell">
            <div className="stat-label">Ready</div>
            <div className="stat-value tone-healthy">{s.nodes.ready}</div>
          </div>
          <div className="stat-cell">
            <div className="stat-label">Not ready</div>
            <div className="stat-value tone-warning">{s.nodes.notReady}</div>
          </div>
          <div className="stat-cell">
            <div className="stat-label">Unreachable</div>
            <div className="stat-value tone-critical">{s.nodes.unreachable}</div>
          </div>
          <div className="stat-cell">
            <div className="stat-label">Cordoned</div>
            <div className="stat-value">{s.nodes.cordoned}</div>
          </div>
        </div>
      </div>

      <div className="section">
        <div className="section-title">Workloads</div>
        <div className="stat-grid">
          <div className="stat-cell">
            <div className="stat-label">Total</div>
            <div className="stat-value">{s.workloads.total}</div>
          </div>
          <div className="stat-cell">
            <div className="stat-label">Running</div>
            <div className="stat-value tone-healthy">{s.workloads.running}</div>
          </div>
          <div className="stat-cell">
            <div className="stat-label">Pending</div>
            <div className="stat-value">{s.workloads.pending}</div>
          </div>
          <div className="stat-cell">
            <div className="stat-label">Starting</div>
            <div className="stat-value">{s.workloads.starting}</div>
          </div>
          <div className="stat-cell">
            <div className="stat-label">Failed</div>
            <div className="stat-value tone-critical">{s.workloads.failed}</div>
          </div>
          <div className="stat-cell">
            <div className="stat-label">Unhealthy</div>
            <div className="stat-value tone-warning">{s.workloads.unhealthy}</div>
          </div>
          <div className="stat-cell">
            <div className="stat-label">Unschedulable</div>
            <div className="stat-value tone-warning">{s.workloads.unschedulable}</div>
          </div>
          <div className="stat-cell">
            <div className="stat-label">Restarts</div>
            <div className="stat-value">{s.workloads.restarts}</div>
          </div>
        </div>
      </div>

      <div className="section">
        <div className="section-title">Deployments &amp; Services</div>
        <div className="stat-grid">
          <div className="stat-cell">
            <div className="stat-label">Deployments</div>
            <div className="stat-value">{s.deployments.total}</div>
            <div className="stat-sub">
              {s.deployments.available} available · {s.deployments.progressing} progressing ·{" "}
              {s.deployments.degraded} degraded
            </div>
          </div>
          <div className="stat-cell">
            <div className="stat-label">Services</div>
            <div className="stat-value">{s.services.total}</div>
            <div className="stat-sub">{s.services.withoutHealthyEndpoints} without healthy endpoints</div>
          </div>
        </div>
      </div>

      <div className="section">
        <div className="section-title">Cluster capacity (Ready nodes)</div>
        <div className="stat-grid">
          <div className="stat-cell">
            <div className="stat-label">CPU allocated</div>
            <div className="stat-value">{formatPercent(s.capacity.cpuAllocated, s.capacity.cpuAllocatable)}</div>
            <div className="stat-sub">
              {formatCPU(s.capacity.cpuAllocated)} / {formatCPU(s.capacity.cpuAllocatable)} allocatable
            </div>
          </div>
          <div className="stat-cell">
            <div className="stat-label">Memory allocated</div>
            <div className="stat-value">{formatPercent(s.capacity.memAllocated, s.capacity.memAllocatable)}</div>
            <div className="stat-sub">
              {formatBytes(s.capacity.memAllocated)} / {formatBytes(s.capacity.memAllocatable)} allocatable
            </div>
          </div>
          <div className="stat-cell">
            <div className="stat-label">CPU used (measured)</div>
            <div className="stat-value">{formatCPU(s.capacity.cpuUsed)}</div>
          </div>
          <div className="stat-cell">
            <div className="stat-label">Memory used (measured)</div>
            <div className="stat-value">{formatBytes(s.capacity.memUsed)}</div>
          </div>
        </div>
      </div>

      <div className="section">
        <div className="section-title">Recent events</div>
        {events.error && <ErrorState error={events.error} onRetry={events.refetch} />}
        {!events.error && events.initialLoading && <LoadingState label="Loading events…" />}
        {!events.error && !events.initialLoading && (
          <div className="table-wrap">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Time</th>
                  <th>Severity</th>
                  <th>Kind</th>
                  <th>Name</th>
                  <th className="wrap">Message</th>
                </tr>
              </thead>
              <tbody>
                {(events.data?.items ?? []).length === 0 && (
                  <tr>
                    <td colSpan={5} className="text-muted">
                      No events recorded yet.
                    </td>
                  </tr>
                )}
                {(events.data?.items ?? []).map((e) => (
                  <tr key={e.id}>
                    <td>{formatDateTime(e.timestamp)}</td>
                    <td>
                      <StatusBadge label={e.severity} tone={severityTone(e.severity)} />
                    </td>
                    <td>{e.kind}</td>
                    <td>{e.name}</td>
                    <td className="wrap">{e.message}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
        <p className="page-subtitle" style={{ marginTop: 8 }}>
          <Link to="/events">View all events →</Link>
        </p>
      </div>
    </div>
  );
}
