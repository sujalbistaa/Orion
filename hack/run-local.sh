#!/usr/bin/env bash
# Starts a single-node control plane and one local agent against ./bin,
# built by `make build`. Ctrl-C stops both and leaves ./.orion for inspection.
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

"$BIN/orion-server" \
	-data-dir "$DATA_DIR/server" \
	-log-format text -log-level "$LOG_LEVEL" \
	-enable-fault-injection &
pids+=($!)

# Give the server a moment to start listening before the agent's first dial.
sleep 1

"$BIN/orion-agent" \
	-node-name local-1 \
	-log-format text -log-level "$LOG_LEVEL" &
pids+=($!)

echo "control plane: http://127.0.0.1:7070   console: http://127.0.0.1:7070"
echo "agent local API: http://127.0.0.1:7090"
echo "orionctl -server http://127.0.0.1:7070 cluster status"
echo "press Ctrl-C to stop"

wait "${pids[@]}"
