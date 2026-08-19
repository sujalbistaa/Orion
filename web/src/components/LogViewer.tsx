import { useEffect, useRef, useState } from "react";
import { authHeaders, externalUrl } from "../api/client";
import { workloadLogsUrl } from "../api/resources";

interface Props {
  workloadName: string;
}

/**
 * Streams GET /api/v1/workloads/{name}/logs (text/plain, pkg/apiserver
 * handlers.go handleWorkloadLogs). Supports a "follow" toggle backed by a
 * genuine chunked read of the live response body — not a fake typing
 * animation.
 */
export function LogViewer({ workloadName }: Props) {
  const [lines, setLines] = useState<string>("");
  const [following, setFollowing] = useState(false);
  const [tail, setTail] = useState(200);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [unavailable, setUnavailable] = useState(false);
  const abortRef = useRef<AbortController | null>(null);
  const preRef = useRef<HTMLPreElement>(null);

  function stop() {
    abortRef.current?.abort();
    abortRef.current = null;
    setFollowing(false);
  }

  async function load(follow: boolean) {
    stop();
    setError(null);
    setUnavailable(false);
    setLoading(true);
    setLines("");
    const controller = new AbortController();
    abortRef.current = controller;
    if (follow) setFollowing(true);

    try {
      const url =
        workloadLogsUrl(workloadName, tail) + (follow ? "&follow=true" : "");
      const res = await fetch(externalUrl(url), {
        headers: authHeaders(),
        signal: controller.signal,
      });
      if (res.status === 501) {
        setUnavailable(true);
        setLoading(false);
        return;
      }
      if (!res.ok || !res.body) {
        const text = await res.text().catch(() => "");
        throw new Error(text || `logs request failed with status ${res.status}`);
      }
      setLoading(false);
      const reader = res.body.getReader();
      const decoder = new TextDecoder();
      for (;;) {
        const { value, done } = await reader.read();
        if (done) break;
        const chunk = decoder.decode(value, { stream: true });
        setLines((prev) => prev + chunk);
      }
    } catch (err) {
      if ((err as Error)?.name !== "AbortError") {
        setError(err instanceof Error ? err.message : String(err));
      }
      setLoading(false);
    } finally {
      if (abortRef.current === controller) {
        setFollowing(false);
        abortRef.current = null;
      }
    }
  }

  useEffect(() => {
    void load(false);
    return () => stop();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [workloadName]);

  useEffect(() => {
    if (preRef.current) preRef.current.scrollTop = preRef.current.scrollHeight;
  }, [lines]);

  if (unavailable) {
    return (
      <p className="text-muted">
        This server is not configured to fetch container logs.
      </p>
    );
  }

  return (
    <div>
      <div className="toolbar">
        <label className="field" style={{ flexDirection: "row", alignItems: "center", gap: 6 }}>
          <span>Tail</span>
          <select
            className="filter-select"
            value={tail}
            onChange={(e) => setTail(Number(e.target.value))}
            disabled={following}
          >
            {[100, 200, 500, 1000, 5000].map((n) => (
              <option key={n} value={n}>
                {n}
              </option>
            ))}
          </select>
        </label>
        <button className="btn btn-sm" onClick={() => void load(false)} disabled={loading && !following}>
          {loading && !following ? "Loading…" : "Reload"}
        </button>
        {following ? (
          <button className="btn btn-sm" onClick={stop}>
            Stop following
          </button>
        ) : (
          <button className="btn btn-sm" onClick={() => void load(true)}>
            Follow
          </button>
        )}
      </div>
      {error && <div className="inline-error">{error}</div>}
      <pre className="pre-log" ref={preRef} aria-label={`Logs for ${workloadName}`}>
        {lines || (loading ? "" : "No log output.")}
      </pre>
    </div>
  );
}
