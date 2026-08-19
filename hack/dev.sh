#!/usr/bin/env bash
# Starts a single-node control plane, one local agent, and the web console
# dev server with live reload. Falls back to just the backend if the web
# console has no package.json yet (`make web-install` not run, or the
# frontend hasn't been built out). Ctrl-C stops everything.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

BIN=./bin
DATA_DIR=${ORION_DATA_DIR:-.orion}
LOG_LEVEL=${ORION_LOG_LEVEL:-info}

for b in orion-server orion-agent; do
	if [[ ! -x "$BIN/$b" ]]; then
		echo "missing $BIN/$b — run 'make build' first" >&2
		exit 1
	fi
done

pids=()
cleanup() {
	trap - EXIT INT TERM
	for pid in "${pids[@]}"; do
		kill "$pid" 2>/dev/null || true
	done
	wait "${pids[@]}" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

mkdir -p "$DATA_DIR"

# -enable-console false: in dev, the console is served by Vite (with API
# proxying and live reload), not the embedded build.
"$BIN/orion-server" \
	-data-dir "$DATA_DIR/server" \
	-log-format text -log-level "$LOG_LEVEL" \
	-enable-console=false \
	-enable-fault-injection &
pids+=($!)

sleep 1

"$BIN/orion-agent" \
	-node-name local-1 \
	-log-format text -log-level "$LOG_LEVEL" &
pids+=($!)

echo "control plane API: http://127.0.0.1:7070"
echo "agent local API: http://127.0.0.1:7090"

if [[ -f web/package.json ]]; then
	(cd web && npm run dev) &
	pids+=($!)
else
	echo "web/package.json not found — skipping the console dev server (backend only)"
fi

echo "press Ctrl-C to stop"
wait "${pids[@]}"
