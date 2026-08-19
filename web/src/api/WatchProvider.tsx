import {
  createContext,
  useContext,
  useEffect,
  useRef,
  useState,
  useCallback,
  type ReactNode,
} from "react";
import { WatchClient } from "./watch";
import { subscribeSettings } from "./settings";
import type { WatchChange } from "./types";

type ChangeListener = (change: WatchChange) => void;
type ResyncListener = () => void;

interface WatchContextValue {
  connected: boolean;
  /** Subscribe to changes for a specific object Kind (e.g. "Node"). */
  onChange: (kind: string, listener: ChangeListener) => () => void;
  /** Subscribe to the "resync" signal — callers should refetch their list. */
  onResync: (listener: ResyncListener) => () => void;
}

const WatchContext = createContext<WatchContextValue | null>(null);

export function WatchProvider({ children }: { children: ReactNode }) {
  const [connected, setConnected] = useState(false);
  const changeListeners = useRef(new Map<string, Set<ChangeListener>>());
  const resyncListeners = useRef(new Set<ResyncListener>());
  const clientRef = useRef<WatchClient | null>(null);

  useEffect(() => {
    function makeClient() {
      return new WatchClient({
        onConnectionChange: setConnected,
        onChange: (change) => {
          const set = changeListeners.current.get(change.kind);
          set?.forEach((l) => l(change));
          const wildcard = changeListeners.current.get("*");
          wildcard?.forEach((l) => l(change));
        },
        onResync: () => {
          resyncListeners.current.forEach((l) => l());
        },
      });
    }

    let client = makeClient();
    clientRef.current = client;
    client.start();

    // Restart the stream when the API base URL or token changes, so a
    // Settings update takes effect without a page reload.
    const unsubscribe = subscribeSettings(() => {
      client.stop();
      client = makeClient();
      clientRef.current = client;
      client.start();
    });

    return () => {
      unsubscribe();
      client.stop();
    };
  }, []);

  const onChange = useCallback((kind: string, listener: ChangeListener) => {
    let set = changeListeners.current.get(kind);
    if (!set) {
      set = new Set();
      changeListeners.current.set(kind, set);
    }
    set.add(listener);
    return () => set!.delete(listener);
  }, []);

  const onResync = useCallback((listener: ResyncListener) => {
    resyncListeners.current.add(listener);
    return () => resyncListeners.current.delete(listener);
  }, []);

  return (
    <WatchContext.Provider value={{ connected, onChange, onResync }}>
      {children}
    </WatchContext.Provider>
  );
}

export function useWatchConnection(): boolean {
  const ctx = useContext(WatchContext);
  return ctx?.connected ?? false;
}

/**
 * Re-runs `refetch` whenever a change of the given Kind(s) arrives on the
 * watch stream, or when the stream signals a resync. Pass an empty array to
 * only react to resync.
 */
export function useWatchRefetch(kinds: string[], refetch: () => void): void {
  const ctx = useContext(WatchContext);
  useEffect(() => {
    if (!ctx) return;
    const unsubs = kinds.map((k) => ctx.onChange(k, () => refetch()));
    const unsubResync = ctx.onResync(() => refetch());
    return () => {
      unsubs.forEach((u) => u());
      unsubResync();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [ctx, kinds.join(",")]);
}
