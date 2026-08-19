// Maps Orion's state enums (pkg/api/v1/state.go) onto the four semantic
// status colors allowed by the design direction: healthy (green), warning
// (amber), critical (red), informational (muted blue), plus neutral (gray).
export type StatusTone = "healthy" | "warning" | "critical" | "info" | "neutral";

export function nodePhaseTone(phase: string): StatusTone {
  switch (phase) {
    case "Ready":
      return "healthy";
    case "Registering":
    case "Draining":
      return "info";
    case "NotReady":
      return "warning";
    case "Unreachable":
      return "critical";
    case "Decommissioned":
      return "neutral";
    default:
      return "neutral";
  }
}

export function workloadPhaseTone(phase: string): StatusTone {
  switch (phase) {
    case "Running":
    case "Succeeded":
      return "healthy";
    case "Pending":
    case "Scheduled":
    case "Starting":
    case "Terminating":
      return "info";
    case "Failed":
      return "critical";
    case "Terminated":
      return "neutral";
    default:
      return "neutral";
  }
}

export function healthTone(health: string): StatusTone {
  switch (health) {
    case "Healthy":
      return "healthy";
    case "Unhealthy":
      return "critical";
    default:
      return "neutral";
  }
}

export function deploymentPhaseTone(phase: string): StatusTone {
  switch (phase) {
    case "Available":
      return "healthy";
    case "Progressing":
      return "info";
    case "Degraded":
      return "critical";
    default:
      return "neutral";
  }
}

export function severityTone(sev: string): StatusTone {
  switch (sev) {
    case "Info":
      return "info";
    case "Warning":
      return "warning";
    case "Critical":
      return "critical";
    default:
      return "neutral";
  }
}

export function runStateTone(state: string): StatusTone {
  switch (state) {
    case "Succeeded":
      return "healthy";
    case "Failed":
      return "critical";
    case "Aborted":
      return "warning";
    case "Pending":
      return "neutral";
    default:
      return "info";
  }
}

export function memberRoleTone(role: string): StatusTone {
  switch (role) {
    case "Leader":
      return "healthy";
    case "Follower":
      return "info";
    case "Candidate":
      return "warning";
    default:
      return "neutral";
  }
}
