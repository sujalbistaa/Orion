import { useState } from "react";
import { createService, deleteService, listServices } from "../api/resources";
import { useAsync } from "../hooks/useAsync";
import { useWatchRefetch } from "../api/WatchProvider";
import { ErrorState, LoadingState, EmptyState } from "../components/States";
import { StatusBadge } from "../components/StatusBadge";
import { Modal } from "../components/Modal";
import { ConfirmDialog } from "../components/ConfirmDialog";
import { useToast, describeError } from "../components/Toast";
import { extractFieldErrors, fieldError } from "../utils/fieldErrors";
import type { FieldError, Service } from "../api/types";

export function Services() {
  const services = useAsync(() => listServices(), []);
  useWatchRefetch(["Service"], services.refetch);
  const toast = useToast();
  const [creating, setCreating] = useState(false);
  const [target, setTarget] = useState<Service | null>(null);
  const [busy, setBusy] = useState(false);

  async function doDelete() {
    if (!target) return;
    setBusy(true);
    try {
      await deleteService(target.name);
      toast.push("success", `${target.name} deleted`);
      setTarget(null);
      services.refetch();
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
          <h1>Services</h1>
          <p className="page-subtitle">Stable names in front of a changing set of workload endpoints.</p>
        </div>
        <div className="page-actions">
          <button className="btn" onClick={services.refetch}>
            Refresh
          </button>
          <button className="btn btn-primary" onClick={() => setCreating(true)}>
            New service
          </button>
        </div>
      </div>

      {services.error && <ErrorState error={services.error} onRetry={services.refetch} />}
      {!services.error && services.initialLoading && <LoadingState label="Loading services…" />}
      {!services.error && !services.initialLoading && (services.data?.items.length ?? 0) === 0 && (
        <EmptyState
          title="No services"
          detail="Create a service to load-balance across workloads matching a label selector."
          action={
            <button className="btn btn-primary" onClick={() => setCreating(true)}>
              New service
            </button>
          }
        />
      )}
      {!services.error && !services.initialLoading && (services.data?.items.length ?? 0) > 0 && (
        <div className="table-wrap">
          <table className="data-table">
            <thead>
              <tr>
                <th>Name</th>
                <th className="num">Port</th>
                <th className="num">Target port</th>
                <th>Strategy</th>
                <th>Selector</th>
                <th className="num">Endpoints healthy/total</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {services.data!.items.map((s) => (
                <tr key={s.uid}>
                  <td>{s.name}</td>
                  <td className="num">{s.spec.port}</td>
                  <td className="num">{s.spec.targetPort}</td>
                  <td>{s.spec.strategy}</td>
                  <td className="mono">
                    {Object.entries(s.spec.selector)
                      .map(([k, v]) => `${k}=${v}`)
                      .join(", ")}
                  </td>
                  <td className="num">
                    <StatusBadge
                      label={`${s.status.healthyEndpoints}/${s.status.totalEndpoints}`}
                      tone={
                        s.status.totalEndpoints === 0
                          ? "neutral"
                          : s.status.healthyEndpoints === s.status.totalEndpoints
                            ? "healthy"
                            : s.status.healthyEndpoints === 0
                              ? "critical"
                              : "warning"
                      }
                    />
                  </td>
                  <td>
                    <button className="btn btn-sm btn-danger" onClick={() => setTarget(s)}>
                      Delete
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {(services.data?.items.length ?? 0) > 0 && (
        <div className="section">
          <div className="section-title">Endpoints</div>
          {services.data!.items.map((s) => (
            <div key={s.uid} className="panel" style={{ marginBottom: 12 }}>
              <div className="panel-header">{s.name}</div>
              <div className="table-wrap" style={{ border: "none" }}>
                <table className="data-table">
                  <thead>
                    <tr>
                      <th>Workload</th>
                      <th>Node</th>
                      <th>Address</th>
                      <th>Health</th>
                      <th>Ready</th>
                    </tr>
                  </thead>
                  <tbody>
                    {s.status.endpoints.length === 0 && (
                      <tr>
                        <td colSpan={5} className="text-muted">
                          No endpoints matched by this service's selector yet.
                        </td>
                      </tr>
                    )}
                    {s.status.endpoints.map((ep) => (
                      <tr key={ep.workloadUid}>
                        <td>{ep.workloadName}</td>
                        <td>{ep.nodeName}</td>
                        <td className="mono">
                          {ep.address}:{ep.port}
                        </td>
                        <td>
                          <StatusBadge
                            label={ep.health}
                            tone={ep.health === "Healthy" ? "healthy" : ep.health === "Unhealthy" ? "critical" : "neutral"}
                          />
                        </td>
                        <td>{ep.ready ? "Yes" : "No"}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          ))}
        </div>
      )}

      {creating && (
        <CreateServiceModal
          onClose={() => setCreating(false)}
          onCreated={() => {
            setCreating(false);
            services.refetch();
            toast.push("success", "Service created");
          }}
        />
      )}

      {target && (
        <ConfirmDialog
          title={`Delete ${target.name}?`}
          danger
          confirmLabel="Delete service"
          busy={busy}
          onConfirm={doDelete}
          onCancel={() => setTarget(null)}
          body={<p>Traffic routed through this service will stop resolving immediately.</p>}
        />
      )}
    </div>
  );
}

function CreateServiceModal({ onClose, onCreated }: { onClose: () => void; onCreated: () => void }) {
  const [name, setName] = useState("");
  const [port, setPort] = useState("8080");
  const [targetPort, setTargetPort] = useState("8080");
  const [strategy, setStrategy] = useState<"RoundRobin" | "LeastConnections">("RoundRobin");
  const [selectorText, setSelectorText] = useState("app=");
  const [fields, setFields] = useState<FieldError[]>([]);
  const [submitError, setSubmitError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  function parseSelector(): Record<string, string> {
    const out: Record<string, string> = {};
    for (const pair of selectorText.split(",")) {
      const [k, v] = pair.split("=").map((s) => s.trim());
      if (k) out[k] = v ?? "";
    }
    return out;
  }

  async function submit() {
    setBusy(true);
    setSubmitError(null);
    setFields([]);
    try {
      await createService({
        name,
        spec: {
          selector: parseSelector(),
          port: Number(port),
          targetPort: Number(targetPort),
          strategy,
        },
      });
      onCreated();
    } catch (err) {
      const fe = extractFieldErrors(err);
      if (fe.length > 0) setFields(fe);
      else setSubmitError(describeError(err));
    } finally {
      setBusy(false);
    }
  }

  return (
    <Modal title="New service" onClose={onClose}>
      <form
        className="form-grid"
        onSubmit={(e) => {
          e.preventDefault();
          void submit();
        }}
      >
        {submitError && <div className="inline-error">{submitError}</div>}
        <div className={`field ${fieldError(fields, "metadata.name") ? "has-error" : ""}`}>
          <label htmlFor="svc-name">Name</label>
          <input id="svc-name" type="text" required value={name} onChange={(e) => setName(e.target.value)} placeholder="web" />
          {fieldError(fields, "metadata.name") && <span className="inline-error">{fieldError(fields, "metadata.name")}</span>}
        </div>

        <div className={`field ${fieldError(fields, "spec.selector") ? "has-error" : ""}`}>
          <label htmlFor="svc-selector">Selector</label>
          <input
            id="svc-selector"
            type="text"
            required
            value={selectorText}
            onChange={(e) => setSelectorText(e.target.value)}
            placeholder="app=web,tier=frontend"
          />
          <span className="hint">Comma-separated key=value pairs matched against workload labels.</span>
          {fieldError(fields, "spec.selector") && <span className="inline-error">{fieldError(fields, "spec.selector")}</span>}
        </div>

        <div className="form-row">
          <div className={`field ${fieldError(fields, "spec.port") ? "has-error" : ""}`}>
            <label htmlFor="svc-port">Port</label>
            <input id="svc-port" type="number" min="1024" max="65535" required value={port} onChange={(e) => setPort(e.target.value)} />
            {fieldError(fields, "spec.port") && <span className="inline-error">{fieldError(fields, "spec.port")}</span>}
          </div>
          <div className={`field ${fieldError(fields, "spec.targetPort") ? "has-error" : ""}`}>
            <label htmlFor="svc-target-port">Target port</label>
            <input
              id="svc-target-port"
              type="number"
              min="1"
              max="65535"
              required
              value={targetPort}
              onChange={(e) => setTargetPort(e.target.value)}
            />
            {fieldError(fields, "spec.targetPort") && (
              <span className="inline-error">{fieldError(fields, "spec.targetPort")}</span>
            )}
          </div>
          <div className="field">
            <label htmlFor="svc-strategy">Strategy</label>
            <select id="svc-strategy" value={strategy} onChange={(e) => setStrategy(e.target.value as typeof strategy)}>
              <option value="RoundRobin">Round robin</option>
              <option value="LeastConnections">Least connections</option>
            </select>
          </div>
        </div>

        <div className="modal-footer" style={{ padding: 0, borderTop: "none", marginTop: 4 }}>
          <button type="button" className="btn" onClick={onClose} disabled={busy}>
            Cancel
          </button>
          <button type="submit" className="btn btn-primary" disabled={busy}>
            {busy ? "Creating…" : "Create service"}
          </button>
        </div>
      </form>
    </Modal>
  );
}
