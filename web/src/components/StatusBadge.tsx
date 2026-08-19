import type { StatusTone } from "../utils/status";

export function StatusBadge({
  label,
  tone,
}: {
  label: string;
  tone: StatusTone;
}) {
  return <span className={`badge badge-${tone}`}>{label}</span>;
}
