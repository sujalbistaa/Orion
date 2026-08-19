// Formatting helpers. Wire values are raw numbers/ISO strings (see
// pkg/api/v1/resource.go); everything human-readable is derived here, never
// stored.

export function formatCPU(milli: number): string {
  if (milli === 0) return "0";
  if (milli % 1000 === 0) return `${milli / 1000} vCPU`;
  return `${(milli / 1000).toFixed(2)} vCPU`;
}

const BYTE_UNITS = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];

export function formatBytes(bytes: number): string {
  if (bytes === 0) return "0 B";
  const sign = bytes < 0 ? "-" : "";
  let n = Math.abs(bytes);
  let unit = 0;
  while (n >= 1024 && unit < BYTE_UNITS.length - 1) {
    n /= 1024;
    unit++;
  }
  const digits = unit === 0 ? 0 : n < 10 ? 2 : 1;
  // Trim trailing zero precision ("1.50" -> "1.5", "256.00" -> "256") so
  // values don't carry meaningless zeros.
  const formatted = n.toFixed(digits).replace(/(\.\d*?)0+$/, "$1").replace(/\.$/, "");
  return `${sign}${formatted} ${BYTE_UNITS[unit]}`;
}

export function formatResources(r: { cpu: number; memory: number }): string {
  return `${formatCPU(r.cpu)} / ${formatBytes(r.memory)}`;
}

/** Renders a percentage, clamped and rounded, for "42%" style displays. */
export function formatPercent(used: number, total: number): string {
  if (total <= 0) return "—";
  const pct = (used / total) * 100;
  return `${Math.max(0, Math.min(100, Math.round(pct)))}%`;
}

export function formatDateTime(iso?: string): string {
  if (!iso) return "—";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return "—";
  return d.toLocaleString(undefined, {
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

/** Renders an ISO timestamp as a compact relative age, e.g. "3d", "12m". */
export function formatAge(iso?: string): string {
  if (!iso) return "—";
  const then = new Date(iso).getTime();
  if (Number.isNaN(then)) return "—";
  const seconds = Math.max(0, Math.floor((Date.now() - then) / 1000));
  if (seconds < 60) return `${seconds}s`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h`;
  const days = Math.floor(hours / 24);
  if (days < 365) return `${days}d`;
  const years = Math.floor(days / 365);
  return `${years}y`;
}

/** Best-effort humanization of a Go duration string like "1h30m0s" -> "1h 30m". */
export function formatGoDuration(s?: string): string {
  if (!s) return "—";
  const match = s.match(
    /^(-?)(?:(\d+)h)?(?:(\d+)m)?(?:(\d+(?:\.\d+)?)s)?(?:(\d+)ms)?$/,
  );
  if (!match) return s;
  const [, neg, h, m, sec, ms] = match;
  const parts: string[] = [];
  if (h) parts.push(`${h}h`);
  if (m) parts.push(`${m}m`);
  // A trailing zero-seconds component (Go prints "1h0m0s") is noise once a
  // larger unit is already shown; keep it only when seconds is all we have.
  if (sec && (Number(sec) !== 0 || parts.length === 0)) parts.push(`${Number(sec)}s`);
  if (parts.length === 0 && ms) parts.push(`${ms}ms`);
  if (parts.length === 0) return "0s";
  return (neg ? "-" : "") + parts.join(" ");
}
