import { externalUrl } from "../api/client";
import { getSummary } from "../api/resources";
import { useAsync } from "../hooks/useAsync";
import { useWatchRefetch } from "../api/WatchProvider";
import { ErrorState, LoadingState } from "../components/States";
import { formatBytes, formatCPU, formatPercent } from "../utils/format";

export function Observability() {
  const summary = useAsync(() => getSummary(), []);
  useWatchRefetch(["Node", "Workload", "Deployment", "Service"], summary.refetch);
  const metricsUrl = externalUrl("/metrics");

  return (
    <div className="page">
      <div className="page-header">
        <div>
          <h1>Observability</h1>
          <p className="page-subtitle">Operational signal Orion exposes today: a Prometheus scrape endpoint and live cluster totals.</p>
        </div>
        <div className="page-actions">
          <button className="btn" onClick={summary.refetch}>
            Refresh
          </button>
        </div>
      </div>

      <div className="section">
        <div className="section-title">Metrics endpoint</div>
        <div className="panel">
          <div className="panel-body">
            <p>
              Orion exposes cluster and API metrics in Prometheus text format at{" "}
              <code className="mono">/metrics</code>. This console does not parse or chart that feed — point a
              Prometheus server or a browser at it directly.
            </p>
            <p style={{ marginTop: 10 }}>
              <a href={metricsUrl} target="_blank" rel="noreferrer" className="btn btn-sm">
                Open /metrics ↗
              </a>
            </p>
            <p className="hint" style={{ marginTop: 8 }}>
              If this server was not started with a metrics registry, the endpoint will 404 — that's expected and
              not a console bug.
            </p>
          </div>
        </div>
      </div>

      <div className="section">
        <div className="section-title">Live cluster signal</div>
        {summary.error && <ErrorState error={summary.error} onRetry={summary.refetch} />}
        {!summary.error && summary.initialLoading && <LoadingState label="Loading summary…" />}
        {!summary.error && summary.data && (
          <>
            <div className="stat-grid">
              <div className="stat-cell">
                <div className="stat-label">CPU allocation</div>
                <div className="stat-value">
                  {formatPercent(summary.data.capacity.cpuAllocated, summary.data.capacity.cpuAllocatable)}
                </div>
                <div className="progress-bar" style={{ marginTop: 6 }}>
                  <div
                    className="progress-bar-fill"
                    style={{
                      width: formatPercent(summary.data.capacity.cpuAllocated, summary.data.capacity.cpuAllocatable),
                    }}
                  />
                </div>
                <div className="stat-sub">
                  {formatCPU(summary.data.capacity.cpuAllocated)} / {formatCPU(summary.data.capacity.cpuAllocatable)}
                </div>
              </div>
              <div className="stat-cell">
                <div className="stat-label">Memory allocation</div>
                <div className="stat-value">
                  {formatPercent(summary.data.capacity.memAllocated, summary.data.capacity.memAllocatable)}
                </div>
                <div className="progress-bar" style={{ marginTop: 6 }}>
                  <div
                    className="progress-bar-fill"
                    style={{
                      width: formatPercent(summary.data.capacity.memAllocated, summary.data.capacity.memAllocatable),
                    }}
                  />
                </div>
                <div className="stat-sub">
                  {formatBytes(summary.data.capacity.memAllocated)} / {formatBytes(summary.data.capacity.memAllocatable)}
                </div>
              </div>
              <div className="stat-cell">
                <div className="stat-label">CPU used (measured)</div>
                <div className="stat-value">{formatCPU(summary.data.capacity.cpuUsed)}</div>
              </div>
              <div className="stat-cell">
                <div className="stat-label">Memory used (measured)</div>
                <div className="stat-value">{formatBytes(summary.data.capacity.memUsed)}</div>
              </div>
            </div>

            <div className="stat-grid" style={{ marginTop: 12 }}>
              <div className="stat-cell">
                <div className="stat-label">Unhealthy workloads</div>
                <div className={`stat-value ${summary.data.workloads.unhealthy > 0 ? "tone-warning" : ""}`}>
                  {summary.data.workloads.unhealthy}
                </div>
              </div>
              <div className="stat-cell">
                <div className="stat-label">Unschedulable workloads</div>
                <div className={`stat-value ${summary.data.workloads.unschedulable > 0 ? "tone-warning" : ""}`}>
                  {summary.data.workloads.unschedulable}
                </div>
              </div>
              <div className="stat-cell">
                <div className="stat-label">Restarts (cumulative)</div>
                <div className="stat-value">{summary.data.workloads.restarts}</div>
              </div>
              <div className="stat-cell">
                <div className="stat-label">Services without healthy endpoints</div>
                <div className={`stat-value ${summary.data.services.withoutHealthyEndpoints > 0 ? "tone-critical" : ""}`}>
                  {summary.data.services.withoutHealthyEndpoints}
                </div>
              </div>
            </div>
          </>
        )}
      </div>
    </div>
  );
}
