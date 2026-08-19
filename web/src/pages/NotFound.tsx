import { Link } from "react-router-dom";

export function NotFound() {
  return (
    <div className="page">
      <div className="state-block">
        <div className="state-title">Page not found</div>
        <p>
          <Link to="/">Return to Overview</Link>
        </p>
      </div>
    </div>
  );
}
