import { useCallback, useEffect, useRef, useState } from "react";

export interface AsyncState<T> {
  data: T | undefined;
  loading: boolean;
  /** True only on the very first load, so refetches don't blank the page. */
  initialLoading: boolean;
  error: Error | undefined;
  refetch: () => void;
}

/**
 * Runs an async fetcher on mount and whenever `deps` change, exposing
 * loading/error/data state plus a manual refetch. Guards against setting
 * state after unmount and against a slow, superseded request overwriting a
 * newer one.
 */
export function useAsync<T>(
  fetcher: () => Promise<T>,
  deps: React.DependencyList = [],
): AsyncState<T> {
  const [data, setData] = useState<T>();
  const [loading, setLoading] = useState(true);
  const [initialLoading, setInitialLoading] = useState(true);
  const [error, setError] = useState<Error>();
  const requestId = useRef(0);
  const mounted = useRef(true);
  const fetcherRef = useRef(fetcher);
  fetcherRef.current = fetcher;

  useEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
    };
  }, []);

  const run = useCallback(() => {
    const id = ++requestId.current;
    setLoading(true);
    fetcherRef
      .current()
      .then((result) => {
        if (!mounted.current || id !== requestId.current) return;
        setData(result);
        setError(undefined);
      })
      .catch((err: unknown) => {
        if (!mounted.current || id !== requestId.current) return;
        setError(err instanceof Error ? err : new Error(String(err)));
      })
      .finally(() => {
        if (!mounted.current || id !== requestId.current) return;
        setLoading(false);
        setInitialLoading(false);
      });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, deps);

  useEffect(() => {
    run();
  }, [run]);

  return { data, loading, initialLoading, error, refetch: run };
}
