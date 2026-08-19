// Thin fetch wrapper for Orion's HTTP API. Every request applies the
// configured base URL and bearer token (see ./settings.ts); every failure is
// normalized into an ApiError carrying the server's structured error body
// (pkg/apiserver/json.go errorEnvelope) so callers can show field-level
// validation errors instead of a generic message.
import type { APIError, ErrorEnvelope, FieldError } from "./types";
import { getApiBaseUrl, getApiToken } from "./settings";

export class ApiError extends Error {
  status: number;
  code: string;
  fields?: FieldError[];
  leader?: string;

  constructor(status: number, body: APIError, leader?: string) {
    super(body.message || `request failed with status ${status}`);
    this.name = "ApiError";
    this.status = status;
    this.code = body.code;
    this.fields = body.fields;
    this.leader = leader;
  }
}

/** Raised when the request never reached the server (network error, CORS, etc). */
export class NetworkError extends Error {
  constructor(cause: unknown) {
    super(
      cause instanceof Error
        ? `network error: ${cause.message}`
        : "network error",
    );
    this.name = "NetworkError";
  }
}

function buildUrl(path: string): string {
  const base = getApiBaseUrl();
  return base ? `${base}${path}` : path;
}

async function request<T>(
  path: string,
  init: RequestInit = {},
): Promise<T> {
  const headers = new Headers(init.headers);
  const token = getApiToken();
  if (token) headers.set("Authorization", `Bearer ${token}`);
  if (init.body && !headers.has("Content-Type")) {
    headers.set("Content-Type", "application/json");
  }

  let res: Response;
  try {
    res = await fetch(buildUrl(path), { ...init, headers });
  } catch (err) {
    throw new NetworkError(err);
  }

  if (res.status === 204) {
    return undefined as T;
  }

  const contentType = res.headers.get("Content-Type") ?? "";
  const isJson = contentType.includes("application/json");

  if (!res.ok) {
    const leader = res.headers.get("X-Orion-Leader") ?? undefined;
    if (isJson) {
      try {
        const envelope = (await res.json()) as ErrorEnvelope;
        throw new ApiError(res.status, envelope.error, leader);
      } catch (err) {
        if (err instanceof ApiError) throw err;
        // fall through to generic error below
      }
    }
    throw new ApiError(
      res.status,
      { code: "unknown", message: res.statusText || `HTTP ${res.status}` },
      leader,
    );
  }

  if (!isJson) {
    // e.g. text/plain logs, or an empty body.
    return undefined as T;
  }
  return (await res.json()) as T;
}

export const api = {
  get: <T>(path: string) => request<T>(path, { method: "GET" }),
  post: <T>(path: string, body?: unknown) =>
    request<T>(path, {
      method: "POST",
      body: body !== undefined ? JSON.stringify(body) : undefined,
    }),
  put: <T>(path: string, body?: unknown) =>
    request<T>(path, {
      method: "PUT",
      body: body !== undefined ? JSON.stringify(body) : undefined,
    }),
  delete: <T>(path: string) => request<T>(path, { method: "DELETE" }),
};

/** Builds an absolute URL for links out of the app (e.g. /metrics). */
export function externalUrl(path: string): string {
  const base = getApiBaseUrl();
  if (base) return `${base}${path}`;
  return `${window.location.origin}${path}`;
}

export function authHeaders(): Record<string, string> {
  const token = getApiToken();
  return token ? { Authorization: `Bearer ${token}` } : {};
}
