#!/usr/bin/env bash
# deployment-gate: real upstream, real credentials, intentionally excluded from CI.
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
RUN_DIR=$(mktemp -d /tmp/orihsus-e2e-real.XXXXXX)
TARGET_DIR=$(mktemp -d /tmp/orihsus-target-e2e-real.XXXXXX)
PID=""

cleanup() {
  local status=$?
  trap - EXIT INT TERM
  if [[ -n "$PID" ]] && kill -0 "$PID" 2>/dev/null; then
    kill -TERM "$PID" 2>/dev/null || true
    for _ in {1..50}; do
      kill -0 "$PID" 2>/dev/null || break
      sleep 0.1
    done
    kill -KILL "$PID" 2>/dev/null || true
    wait "$PID" 2>/dev/null || true
  fi
  rm -rf -- "$RUN_DIR" "$TARGET_DIR"
  exit "$status"
}
trap cleanup EXIT INT TERM
umask 077

if [[ -n "${ORIHSUS_E2E_KEY:-}" && -n "${ORIHSUS_E2E_KEY_FILE:-}" ]]; then
  echo "deployment-gate: set only one of ORIHSUS_E2E_KEY or ORIHSUS_E2E_KEY_FILE" >&2
  exit 2
elif [[ -n "${ORIHSUS_E2E_KEY:-}" ]]; then
  printf '%s\n' "$ORIHSUS_E2E_KEY" >"$RUN_DIR/upstream-key"
  unset ORIHSUS_E2E_KEY
elif [[ -n "${ORIHSUS_E2E_KEY_FILE:-}" ]]; then
  cp -- "$ORIHSUS_E2E_KEY_FILE" "$RUN_DIR/upstream-key"
  unset ORIHSUS_E2E_KEY_FILE
else
  echo "deployment-gate: ORIHSUS_E2E_KEY or ORIHSUS_E2E_KEY_FILE is required" >&2
  exit 2
fi
chmod 600 "$RUN_DIR/upstream-key"

python3 "$ROOT_DIR/tools/e2e-real/make-config.py" \
  "$RUN_DIR/upstream-key" "$RUN_DIR/config.yaml" "$RUN_DIR/credentials.json" \
  "$RUN_DIR/audit.jsonl" "$RUN_DIR/usage"
chmod 600 "$RUN_DIR/config.yaml" "$RUN_DIR/credentials.json"

echo "deployment-gate: building orihsus with target directory under /tmp"
CARGO_TARGET_DIR="$TARGET_DIR" cargo build --locked --manifest-path "$ROOT_DIR/Cargo.toml"

"$TARGET_DIR/debug/orihsus" --config "$RUN_DIR/config.yaml" \
  >"$RUN_DIR/stdout.log" 2>"$RUN_DIR/stderr.log" &
PID=$!

for _ in {1..100}; do
  kill -0 "$PID" 2>/dev/null || {
    echo "deployment-gate: orihsus exited during startup" >&2
    exit 1
  }
  if curl --silent --fail --max-time 1 http://127.0.0.1:18082/healthz >/dev/null; then
    break
  fi
  sleep 0.1
done
curl --silent --fail --max-time 2 http://127.0.0.1:18082/readyz >/dev/null

python3 "$ROOT_DIR/tools/e2e-real/verify.py" \
  "$RUN_DIR/credentials.json" "$RUN_DIR/audit.jsonl" "$PID"

if ! kill -0 "$PID" 2>/dev/null; then
  echo "deployment-gate: orihsus did not survive the request sequence" >&2
  exit 1
fi
if grep -Eqi 'panicked|thread .* panic|config reload refused' "$RUN_DIR/stderr.log"; then
  echo "deployment-gate: panic or refusal marker found in gateway diagnostics" >&2
  exit 1
fi

echo "deployment-gate: PASS (real ox-alpha-free upstream, secrets and artifacts will now be removed)"
