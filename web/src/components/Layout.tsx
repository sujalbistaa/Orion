import { NavLink, Outlet } from "react-router-dom";
import { useWatchConnection } from "../api/WatchProvider";

const NAV = [
  { to: "/", label: "Overview", end: true },
  { to: "/clusters", label: "Clusters" },
  { to: "/nodes", label: "Nodes" },
  { to: "/workloads", label: "Workloads" },
  { to: "/deployments", label: "Deployments" },
  { to: "/services", label: "Services" },
  { to: "/events", label: "Events" },
  { to: "/observability", label: "Observability" },
  { to: "/faults", label: "Fault Injection" },
  { to: "/settings", label: "Settings" },
];

export function Layout() {
  const connected = useWatchConnection();

  return (
    <div className="app-shell">
      <nav className="sidebar" aria-label="Primary">
        <div className="sidebar-header">
          <div className="sidebar-title">ORION</div>
          <div className="sidebar-subtitle">Cluster console</div>
        </div>
        <div className="sidebar-nav">
          {NAV.map((item) => (
            <NavLink
              key={item.to}
              to={item.to}
              end={item.end}
              className={({ isActive }) =>
                "sidebar-link" + (isActive ? " active" : "")
              }
            >
              {item.label}
            </NavLink>
          ))}
        </div>
        <div className="sidebar-footer">
          <span
            className={`conn-dot ${connected ? "connected" : "disconnected"}`}
            aria-hidden="true"
          />
          <span>{connected ? "Live" : "Reconnecting…"}</span>
        </div>
      </nav>
      <div className="main">
        <Outlet />
      </div>
    </div>
  );
}
