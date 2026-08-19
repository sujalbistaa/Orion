// Shared form fields for a WorkloadSpec, used by the deployment create form.
// Deliberately limited to the fields the brief calls out (image, resources,
// ports) plus the object name and replica count — no invented fields.
import type { FieldError } from "../api/types";
import { fieldError } from "../utils/fieldErrors";

export interface PortDraft {
  container: string;
  host: string;
  protocol: "tcp" | "udp";
}

export interface WorkloadSpecDraft {
  cpuRequest: string; // vCPU, e.g. "0.5"
  memRequest: string; // value in the chosen unit
  memRequestUnit: "Mi" | "Gi";
  setLimits: boolean;
  cpuLimit: string;
  memLimit: string;
  memLimitUnit: "Mi" | "Gi";
  ports: PortDraft[];
}

export function emptyWorkloadSpecDraft(): WorkloadSpecDraft {
  return {
    cpuRequest: "0.25",
    memRequest: "256",
    memRequestUnit: "Mi",
    setLimits: false,
    cpuLimit: "",
    memLimit: "",
    memLimitUnit: "Mi",
    ports: [],
  };
}

const UNIT_MULT: Record<"Mi" | "Gi", number> = {
  Mi: 1024 * 1024,
  Gi: 1024 * 1024 * 1024,
};

export function draftToResourceSpec(d: WorkloadSpecDraft) {
  const request = {
    cpu: Math.round(parseFloat(d.cpuRequest || "0") * 1000),
    memory: Math.round(parseFloat(d.memRequest || "0") * UNIT_MULT[d.memRequestUnit]),
  };
  if (!d.setLimits) return { request };
  const limit = {
    cpu: Math.round(parseFloat(d.cpuLimit || "0") * 1000),
    memory: Math.round(parseFloat(d.memLimit || "0") * UNIT_MULT[d.memLimitUnit]),
  };
  return { request, limit };
}

export function draftToPorts(ports: PortDraft[]) {
  return ports
    .filter((p) => p.container.trim() !== "")
    .map((p) => ({
      container: Number(p.container),
      ...(p.host.trim() !== "" ? { host: Number(p.host) } : {}),
      protocol: p.protocol,
    }));
}

export function ResourceFields({
  draft,
  onChange,
  fields,
  prefix,
}: {
  draft: WorkloadSpecDraft;
  onChange: (d: WorkloadSpecDraft) => void;
  fields: FieldError[];
  prefix: string;
}) {
  return (
    <div className="form-grid">
      <div className="form-row">
        <div className={`field ${fieldError(fields, `${prefix}resources.request.cpu`) ? "has-error" : ""}`}>
          <label htmlFor="cpu-request">CPU request (vCPU)</label>
          <input
            id="cpu-request"
            type="number"
            min="0"
            step="0.05"
            value={draft.cpuRequest}
            onChange={(e) => onChange({ ...draft, cpuRequest: e.target.value })}
          />
          {fieldError(fields, `${prefix}resources.request.cpu`) && (
            <span className="inline-error">{fieldError(fields, `${prefix}resources.request.cpu`)}</span>
          )}
        </div>
        <div className={`field ${fieldError(fields, `${prefix}resources.request.memory`) ? "has-error" : ""}`}>
          <label htmlFor="mem-request">Memory request</label>
          <div style={{ display: "flex", gap: 6 }}>
            <input
              id="mem-request"
              type="number"
              min="0"
              step="1"
              value={draft.memRequest}
              onChange={(e) => onChange({ ...draft, memRequest: e.target.value })}
            />
            <select
              value={draft.memRequestUnit}
              onChange={(e) => onChange({ ...draft, memRequestUnit: e.target.value as "Mi" | "Gi" })}
              aria-label="Memory request unit"
            >
              <option value="Mi">MiB</option>
              <option value="Gi">GiB</option>
            </select>
          </div>
          {fieldError(fields, `${prefix}resources.request.memory`) && (
            <span className="inline-error">{fieldError(fields, `${prefix}resources.request.memory`)}</span>
          )}
        </div>
      </div>

      <label className="checkbox-row">
        <input
          type="checkbox"
          checked={draft.setLimits}
          onChange={(e) => onChange({ ...draft, setLimits: e.target.checked })}
        />
        Set explicit resource limits (defaults to request)
      </label>

      {draft.setLimits && (
        <div className="form-row">
          <div className={`field ${fieldError(fields, `${prefix}resources.limit.cpu`) ? "has-error" : ""}`}>
            <label htmlFor="cpu-limit">CPU limit (vCPU)</label>
            <input
              id="cpu-limit"
              type="number"
              min="0"
              step="0.05"
              value={draft.cpuLimit}
              onChange={(e) => onChange({ ...draft, cpuLimit: e.target.value })}
            />
            {fieldError(fields, `${prefix}resources.limit.cpu`) && (
              <span className="inline-error">{fieldError(fields, `${prefix}resources.limit.cpu`)}</span>
            )}
          </div>
          <div className={`field ${fieldError(fields, `${prefix}resources.limit.memory`) ? "has-error" : ""}`}>
            <label htmlFor="mem-limit">Memory limit</label>
            <div style={{ display: "flex", gap: 6 }}>
              <input
                id="mem-limit"
                type="number"
                min="0"
                step="1"
                value={draft.memLimit}
                onChange={(e) => onChange({ ...draft, memLimit: e.target.value })}
              />
              <select
                value={draft.memLimitUnit}
                onChange={(e) => onChange({ ...draft, memLimitUnit: e.target.value as "Mi" | "Gi" })}
                aria-label="Memory limit unit"
              >
                <option value="Mi">MiB</option>
                <option value="Gi">GiB</option>
              </select>
            </div>
            {fieldError(fields, `${prefix}resources.limit.memory`) && (
              <span className="inline-error">{fieldError(fields, `${prefix}resources.limit.memory`)}</span>
            )}
          </div>
        </div>
      )}
    </div>
  );
}

export function PortFields({
  ports,
  onChange,
  fields,
  prefix,
}: {
  ports: PortDraft[];
  onChange: (ports: PortDraft[]) => void;
  fields: FieldError[];
  prefix: string;
}) {
  function update(i: number, patch: Partial<PortDraft>) {
    onChange(ports.map((p, idx) => (idx === i ? { ...p, ...patch } : p)));
  }
  function remove(i: number) {
    onChange(ports.filter((_, idx) => idx !== i));
  }
  function add() {
    onChange([...ports, { container: "", host: "", protocol: "tcp" }]);
  }

  return (
    <div className="field">
      <label>Ports</label>
      {ports.length === 0 && <span className="hint">No ports published. The container may still listen internally.</span>}
      {ports.map((p, i) => (
        <div className="repeat-row" key={i}>
          <input
            type="number"
            placeholder="Container port"
            aria-label={`Port ${i + 1} container port`}
            value={p.container}
            min="1"
            max="65535"
            style={{ width: 130 }}
            onChange={(e) => update(i, { container: e.target.value })}
          />
          <input
            type="number"
            placeholder="Host port (auto)"
            aria-label={`Port ${i + 1} host port`}
            value={p.host}
            min="0"
            max="65535"
            style={{ width: 140 }}
            onChange={(e) => update(i, { host: e.target.value })}
          />
          <select
            aria-label={`Port ${i + 1} protocol`}
            value={p.protocol}
            onChange={(e) => update(i, { protocol: e.target.value as "tcp" | "udp" })}
          >
            <option value="tcp">tcp</option>
            <option value="udp">udp</option>
          </select>
          <button type="button" className="btn btn-sm btn-ghost" onClick={() => remove(i)} aria-label={`Remove port ${i + 1}`}>
            Remove
          </button>
        </div>
      ))}
      <div>
        <button type="button" className="btn btn-sm" onClick={add}>
          Add port
        </button>
      </div>
      {fieldError(fields, `${prefix}ports`) && <span className="inline-error">{fieldError(fields, `${prefix}ports`)}</span>}
    </div>
  );
}
