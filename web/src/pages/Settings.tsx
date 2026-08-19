import { useState } from "react";
import { getApiBaseUrl, getApiToken, setApiBaseUrl, setApiToken } from "../api/settings";
import { getCluster } from "../api/resources";
import { useToast, describeError } from "../components/Toast";

export function Settings() {
  const [baseUrl, setBaseUrl] = useState(getApiBaseUrl());
  const [token, setToken] = useState(getApiToken());
  const [testState, setTestState] = useState<"idle" | "testing" | "ok" | "fail">("idle");
  const [testDetail, setTestDetail] = useState<string | null>(null);
  const toast = useToast();

  function save() {
    setApiBaseUrl(baseUrl);
    setApiToken(token);
    toast.push("success", "Settings saved");
  }

  async function testConnection() {
    setTestState("testing");
    setTestDetail(null);
    // Save first so the test call (and the watch stream reconnect) uses the
    // values currently in the form, not whatever was previously stored.
    setApiBaseUrl(baseUrl);
    setApiToken(token);
    try {
      const res = await getCluster();
      setTestState("ok");
      setTestDetail(`Connected to cluster "${res.cluster.name}" (leader: ${res.cluster.leaderId || "none"}).`);
    } catch (err) {
      setTestState("fail");
      setTestDetail(describeError(err));
    }
  }

  return (
    <div className="page">
      <div className="page-header">
        <div>
          <h1>Settings</h1>
          <p className="page-subtitle">Console configuration. Stored in this browser's localStorage and applied to every API request.</p>
        </div>
      </div>

      <form
        className="form-grid"
        style={{ maxWidth: 560 }}
        onSubmit={(e) => {
          e.preventDefault();
          save();
        }}
      >
        <div className="field">
          <label htmlFor="settings-base-url">API base URL</label>
          <input
            id="settings-base-url"
            type="text"
            value={baseUrl}
            onChange={(e) => setBaseUrl(e.target.value)}
            placeholder="(empty = same origin, e.g. proxied by the Vite dev server)"
          />
          <span className="hint">
            Leave empty to use relative paths. In production the console is served by orion-server itself, so requests are
            same-origin.
          </span>
        </div>

        <div className="field">
          <label htmlFor="settings-token">Bearer token</label>
          <input
            id="settings-token"
            type="password"
            autoComplete="off"
            value={token}
            onChange={(e) => setToken(e.target.value)}
            placeholder="(empty = unauthenticated, if the server allows it)"
          />
          <span className="hint">
            Sent as <code>Authorization: Bearer &lt;token&gt;</code> on every request. Required when the server was started
            with <code>ORION_API_TOKEN</code>. Read-only tokens (RoleViewer) can view but not act — write actions will show a
            403 error.
          </span>
        </div>

        <div style={{ display: "flex", gap: 8 }}>
          <button type="submit" className="btn btn-primary">
            Save
          </button>
          <button type="button" className="btn" onClick={testConnection} disabled={testState === "testing"}>
            {testState === "testing" ? "Testing…" : "Test connection"}
          </button>
        </div>

        {testState === "ok" && <div className="state-block" style={{ textAlign: "left", padding: 12 }}>{testDetail}</div>}
        {testState === "fail" && <div className="inline-error">{testDetail}</div>}
      </form>
    </div>
  );
}
