import { useState } from "react";
import { listEvents } from "../api/resources";
import { useAsync } from "../hooks/useAsync";
import { useWatchRefetch } from "../api/WatchProvider";
import { ErrorState, LoadingState, EmptyState } from "../components/States";
import { StatusBadge } from "../components/StatusBadge";
import { formatDateTime } from "../utils/format";
import { severityTone } from "../utils/status";
import type { Event } from "../api/types";

const SEVERITIES = ["Info", "Warning", "Critical"];
const PAGE_SIZE = 50;

export function Events() {
  const [kind, setKind] = useState("");
  const [name, setName] = useState("");
  const [severity, setSeverity] = useState("");
  // Stack of "before" cursors so Back can pop to the previous page.
  const [cursorStack, setCursorStack] = useState<number[]>([]);
  const before = cursorStack[cursorStack.length - 1];

  const events = useAsync(
    () =>
      listEvents({
        kind: kind || undefined,
        name: name || undefined,
        severity: severity || undefined,
        before,
        limit: PAGE_SIZE,
      }),
    [kind, name, severity, before],
  );
  useWatchRefetch(["Event"], events.refetch);

  const items: Event[] = events.data?.items ?? [];
  const oldestId = items.length > 0 ? items[items.length - 1].id : undefined;

  function resetPaging() {
    setCursorStack([]);
  }

  return (
    <div className="page">
      <div className="page-header">
        <div>
          <h1>Events</h1>
          <p className="page-subtitle">The operational audit trail: every state-changing decision the control plane made.</p>
        </div>
        <div className="page-actions">
          <button className="btn" onClick={events.refetch}>
            Refresh
          </button>
        </div>
      </div>

      <div className="toolbar">
        <input
          className="filter-input"
          type="search"
          placeholder="Filter by object name…"
          aria-label="Filter by name"
          value={name}
          onChange={(e) => {
            setName(e.target.value);
            resetPaging();
          }}
        />
        <select
          className="filter-select"
          aria-label="Filter by kind"
          value={kind}
          onChange={(e) => {
            setKind(e.target.value);
            resetPaging();
          }}
        >
          <option value="">All kinds</option>
          <option value="Node">Node</option>
          <option value="Workload">Workload</option>
          <option value="Deployment">Deployment</option>
          <option value="Service">Service</option>
          <option value="Experiment">Experiment</option>
        </select>
        <select
          className="filter-select"
          aria-label="Filter by severity"
          value={severity}
          onChange={(e) => {
            setSeverity(e.target.value);
            resetPaging();
          }}
        >
          <option value="">All severities</option>
          {SEVERITIES.map((s) => (
            <option key={s} value={s}>
              {s}
            </option>
          ))}
        </select>
        <span className="spacer" />
        <button className="btn btn-sm" disabled={cursorStack.length === 0} onClick={() => setCursorStack((s) => s.slice(0, -1))}>
          ← Newer
        </button>
        <button className="btn btn-sm" disabled={items.length < PAGE_SIZE} onClick={() => oldestId && setCursorStack((s) => [...s, oldestId])}>
          Older →
        </button>
      </div>

      {events.error && <ErrorState error={events.error} onRetry={events.refetch} />}
      {!events.error && events.initialLoading && <LoadingState label="Loading events…" />}
      {!events.error && !events.initialLoading && items.length === 0 && (
        <EmptyState title="No events match this filter" detail="Try widening the kind, name or severity filter." />
      )}
      {!events.error && !events.initialLoading && items.length > 0 && (
        <div className="table-wrap">
          <table className="data-table">
            <thead>
              <tr>
                <th className="num">ID</th>
                <th>Time</th>
                <th>Severity</th>
                <th>Source</th>
                <th>Reason</th>
                <th>Kind</th>
                <th>Name</th>
                <th className="wrap">Message</th>
                <th>Actor</th>
              </tr>
            </thead>
            <tbody>
              {items.map((e) => (
                <tr key={e.id}>
                  <td className="num mono">{e.id}</td>
                  <td>{formatDateTime(e.timestamp)}</td>
                  <td>
                    <StatusBadge label={e.severity} tone={severityTone(e.severity)} />
                  </td>
                  <td>{e.source}</td>
                  <td className="mono">{e.reason}</td>
                  <td>{e.kind}</td>
                  <td>{e.name}</td>
                  <td className="wrap">{e.message}</td>
                  <td>{e.actor || "—"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
