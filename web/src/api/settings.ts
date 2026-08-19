// Console configuration: API base URL and bearer token, persisted in
// localStorage. Read by the fetch client on every request so a change on the
// Settings page takes effect immediately, without a reload.

const BASE_URL_KEY = "orion.apiBaseUrl";
const TOKEN_KEY = "orion.apiToken";

/**
 * The API base URL. Empty string means "same origin, relative /api/v1/...
 * paths" — the normal case, since the Vite dev server proxies /api to the
 * backend and the production build is served by orion-server itself.
 */
export function getApiBaseUrl(): string {
  return localStorage.getItem(BASE_URL_KEY) ?? "";
}

export function setApiBaseUrl(url: string): void {
  const trimmed = url.trim().replace(/\/+$/, "");
  if (trimmed === "") {
    localStorage.removeItem(BASE_URL_KEY);
  } else {
    localStorage.setItem(BASE_URL_KEY, trimmed);
  }
  notify();
}

export function getApiToken(): string {
  return localStorage.getItem(TOKEN_KEY) ?? "";
}

export function setApiToken(token: string): void {
  const trimmed = token.trim();
  if (trimmed === "") {
    localStorage.removeItem(TOKEN_KEY);
  } else {
    localStorage.setItem(TOKEN_KEY, trimmed);
  }
  notify();
}

type Listener = () => void;
const listeners = new Set<Listener>();

function notify(): void {
  for (const l of listeners) l();
}

/** Subscribe to settings changes (used by useSyncExternalStore). */
export function subscribeSettings(listener: Listener): () => void {
  listeners.add(listener);
  return () => listeners.delete(listener);
}
