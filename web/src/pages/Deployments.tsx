import { useState } from "react";
import { Link } from "react-router-dom";
import { createDeployment, listDeployments } from "../api/resources";
import { useAsync } from "../hooks/useAsync";
import { useWatchRefetch } from "../api/WatchProvider";
import { ErrorState, LoadingState, EmptyState } from "../components/States";
import { StatusBadge } from "../components/StatusBadge";
import { Modal } from "../components/Modal";
import { useToast, describeError } from "../components/Toast";
import { extractFieldErrors, fieldError } from "../utils/fieldErrors";
import { formatAge } from "../utils/format";
import { deploymentPhaseTone } from "../utils/status";
import {
  ResourceFields,
  PortFields,
  draftToPorts,
  draftToResourceSpec,
  emptyWorkloadSpecDraft,
  type WorkloadSpecDraft,
} from "../components/WorkloadSpecFields";
import type { FieldError } from "../api/types";

export function Deployments() {
  const deployments = useAsync(() => listDeployments(), []);
  useWatchRefetch(["Deployment"], deployments.refetch);
  const toast = useToast();
  const [creating, setCreating] = useState(false);

  return (
    <div className="page">
      <div className="page-header">
        <div>
          <h1>Deployments</h1>
          <p className="page-subtitle">Maintains N replicas of a workload template.</p>
        </div>
        <div className="page-actions">
          <button className="btn" onClick={deployments.refetch}>
            Refresh
          </button>
          <button className="btn btn-primary" onClick={() => setCreating(true)}>
            New deployment
          </button>
        </div>
      </div>

      {deployments.error && <ErrorState error={deployments.error} onRetry={deployments.refetch} />}
      {!deployments.error && deployments.initialLoading && <LoadingState label="Loading deployments…" />}
      {!deployments.error && !deployments.initialLoading && (deployments.data?.items.length ?? 0) === 0 && (
        <EmptyState
          title="No deployments"
          detail="Create a deployment to maintain a set of replicas of a container image."
          action={
            <button className="btn btn-primary" onClick={() => setCreating(true)}>
              New deployment
            </button>
          }
        />
      )}
      {!deployments.error && !deployments.initialLoading && (deployments.data?.items.length ?? 0) > 0 && (
        <div className="table-wrap">
          <table className="data-table">
            <thead>
              <tr>
                <th>Name</th>
                <th>Phase</th>
                <th className="num">Ready / Desired</th>
                <th className="num">Updated</th>
                <th>Image</th>
                <th className="num">Revision</th>
                <th>Age</th>
              </tr>
            </thead>
            <tbody>
              {deployments.data!.items.map((d) => (
                <tr key={d.uid}>
                  <td>
                    <Link to={`/deployments/${encodeURIComponent(d.name)}`}>{d.name}</Link>
                  </td>
                  <td>
                    <StatusBadge label={d.status.phase} tone={deploymentPhaseTone(d.status.phase)} />
                  </td>
                  <td className="num">
                    {d.status.availableReplicas} / {d.status.desiredReplicas}
                  </td>
                  <td className="num">{d.status.updatedReplicas}</td>
                  <td className="mono">{d.spec.template.image}</td>
                  <td className="num">{d.status.revision}</td>
                  <td>{formatAge(d.createdAt)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {creating && (
        <CreateDeploymentModal
          onClose={() => setCreating(false)}
          onCreated={() => {
            setCreating(false);
            deployments.refetch();
            toast.push("success", "Deployment created");
          }}
        />
      )}
    </div>
  );
}

function CreateDeploymentModal({
  onClose,
  onCreated,
}: {
  onClose: () => void;
  onCreated: () => void;
}) {
  const [name, setName] = useState("");
  const [image, setImage] = useState("");
  const [replicas, setReplicas] = useState("1");
  const [spec, setSpec] = useState<WorkloadSpecDraft>(emptyWorkloadSpecDraft());
  const [fields, setFields] = useState<FieldError[]>([]);
  const [submitError, setSubmitError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  async function submit() {
    setBusy(true);
    setSubmitError(null);
    setFields([]);
    try {
      await createDeployment({
        name,
        spec: {
          replicas: Number(replicas),
          template: {
            image,
            resources: draftToResourceSpec(spec),
            ports: draftToPorts(spec.ports),
          },
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
    <Modal title="New deployment" onClose={onClose} wide>
      <form
        className="form-grid"
        onSubmit={(e) => {
          e.preventDefault();
          void submit();
        }}
      >
        {submitError && <div className="inline-error">{submitError}</div>}
        <div className="form-row">
          <div className={`field ${fieldError(fields, "metadata.name") ? "has-error" : ""}`}>
            <label htmlFor="dep-name">Name</label>
            <input
              id="dep-name"
              type="text"
              required
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="web-frontend"
            />
            <span className="hint">Lowercase alphanumeric with '-', DNS-1123 label.</span>
            {fieldError(fields, "metadata.name") && (
              <span className="inline-error">{fieldError(fields, "metadata.name")}</span>
            )}
          </div>
          <div className={`field ${fieldError(fields, "spec.replicas") ? "has-error" : ""}`}>
            <label htmlFor="dep-replicas">Replicas</label>
            <input
              id="dep-replicas"
              type="number"
              min="0"
              max="1000"
              required
              value={replicas}
              onChange={(e) => setReplicas(e.target.value)}
            />
            {fieldError(fields, "spec.replicas") && (
              <span className="inline-error">{fieldError(fields, "spec.replicas")}</span>
            )}
          </div>
        </div>

        <div className={`field ${fieldError(fields, "spec.template.image") ? "has-error" : ""}`}>
          <label htmlFor="dep-image">Image</label>
          <input
            id="dep-image"
            type="text"
            required
            value={image}
            onChange={(e) => setImage(e.target.value)}
            placeholder="registry.example.com/app:1.4.0"
          />
          {fieldError(fields, "spec.template.image") && (
            <span className="inline-error">{fieldError(fields, "spec.template.image")}</span>
          )}
        </div>

        <ResourceFields draft={spec} onChange={setSpec} fields={fields} prefix="spec.template." />
        <PortFields
          ports={spec.ports}
          onChange={(ports) => setSpec({ ...spec, ports })}
          fields={fields}
          prefix="spec.template."
        />

        <div className="modal-footer" style={{ padding: 0, borderTop: "none", marginTop: 4 }}>
          <button type="button" className="btn" onClick={onClose} disabled={busy}>
            Cancel
          </button>
          <button type="submit" className="btn btn-primary" disabled={busy}>
            {busy ? "Creating…" : "Create deployment"}
          </button>
        </div>
      </form>
    </Modal>
  );
}
