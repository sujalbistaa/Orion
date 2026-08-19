// Client for GET /api/v1/watch (pkg/apiserver/watch.go): a Server-Sent Event
// stream of cluster changes.
//
// Implemented on fetch + ReadableStream rather than the browser EventSource
// API because EventSource cannot attach an Authorization header, and Orion's
// API requires `Authorization: Bearer <token>` when a token is configured.
// Reconnection (with backoff) is therefore handled here explicitly, mirroring
// what EventSource would do natively.
import { getApiBaseUrl, getApiToken } from "./settings";
import type { WatchChange, WatchResync, WatchSync } from "./types";

export interface WatchHandlers {
  onSync?: (s: WatchSync) => void;
  onChange?: (c: WatchChange) => void;
  onResync?: (r: WatchResync) => void;
  /** Fired on connect and on every reconnect attempt's outcome. */
  onConnectionChange?: (connected: boolean) => void;
}

const RECONNECT_DELAYS_MS = [500, 1000, 2000, 5000, 10000];

export class WatchClient {
  private handlers: WatchHandlers;
  private abort: AbortController | null = null;
  private stopped = false;
  private attempt = 0;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;

  constructor(handlers: WatchHandlers) {
    this.handlers = handlers;
  }

  start(): void {
    this.stopped = false;
    void this.connect();
  }

  stop(): void {
    this.stopped = true;
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    this.abort?.abort();
  }

  private scheduleReconnect(): void {
    if (this.stopped) return;
    this.handlers.onConnectionChange?.(false);
    const delay =
      RECONNECT_DELAYS_MS[
        Math.min(this.attempt, RECONNECT_DELAYS_MS.length - 1)
      ];
    this.attempt++;
    this.reconnectTimer = setTimeout(() => void this.connect(), delay);
  }

  private async connect(): Promise<void> {
    if (this.stopped) return;
    this.abort = new AbortController();
    const base = getApiBaseUrl();
    const token = getApiToken();
    const headers: Record<string, string> = { Accept: "text/event-stream" };
    if (token) headers.Authorization = `Bearer ${token}`;

    try {
      const res = await fetch(`${base}/api/v1/watch`, {
        headers,
        signal: this.abort.signal,
      });
      if (!res.ok || !res.body) {
        this.scheduleReconnect();
        return;
      }
      this.attempt = 0;
      this.handlers.onConnectionChange?.(true);

      const reader = res.body.getReader();
      const decoder = new TextDecoder();
      let buf = "";

      for (;;) {
        const { value, done } = await reader.read();
        if (done) break;
        buf += decoder.decode(value, { stream: true });

        let sepIndex: number;
        while ((sepIndex = buf.indexOf("\n\n")) !== -1) {
          const frame = buf.slice(0, sepIndex);
          buf = buf.slice(sepIndex + 2);
          this.dispatchFrame(frame);
        }
      }
    } catch (err) {
      if (this.stopped || (err as Error)?.name === "AbortError") return;
    }
    if (!this.stopped) this.scheduleReconnect();
  }

  private dispatchFrame(frame: string): void {
    let event = "message";
    const dataLines: string[] = [];
    for (const line of frame.split("\n")) {
      if (line.startsWith(":")) continue; // comment / keepalive
      if (line.startsWith("event:")) event = line.slice(6).trim();
      else if (line.startsWith("data:")) dataLines.push(line.slice(5).trim());
    }
    if (dataLines.length === 0) return;
    const raw = dataLines.join("\n");
    try {
      const data = JSON.parse(raw);
      switch (event) {
        case "sync":
          this.handlers.onSync?.(data as WatchSync);
          break;
        case "change":
          this.handlers.onChange?.(data as WatchChange);
          break;
        case "resync":
          this.handlers.onResync?.(data as WatchResync);
          break;
      }
    } catch {
      // malformed frame; skip it rather than tearing down the stream
    }
  }
}
