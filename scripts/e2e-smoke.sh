#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BASE_URL="${BASE_URL:-http://localhost:8080}"
CALLBACK_URL="${CALLBACK_URL:-http://webhook-mock:9000/callback}"

log() {
  echo "[e2e $(date +%H:%M:%S)] $*"
}

log_step() {
  log "=== $* ==="
}

log_step "E2E smoke test"
log "gateway=${BASE_URL} callback=${CALLBACK_URL}"

wait_for() {
  local url=$1
  local name=$2
  log "waiting for ${name} (${url})"
  for i in $(seq 1 60); do
    if curl -sf "$url" >/dev/null 2>&1; then
      log "${name} ready"
      return 0
    fi
    sleep 2
  done
  log "TIMEOUT waiting for ${name}"
  return 1
}

wait_for "${BASE_URL}/actuator/health" "inference-gateway"
wait_for "http://localhost:9000/health" "webhook-mock"

REQ_ID="e2e-req-$(date +%s)"
log_step "single request"
log "POST /v1/inference requestId=${REQ_ID}"
curl -sf -X POST "${BASE_URL}/v1/inference" \
  -H 'Content-Type: application/json' \
  -d "{\"requestId\":\"${REQ_ID}\",\"model\":\"model-a\",\"estimatedTokens\":100,\"payload\":{\"prompt\":\"hi\"}}" \
  | tee /tmp/e2e-single.json
echo ""

log "polling GET /v1/requests/${REQ_ID}"
for i in $(seq 1 60); do
  STATE=$(curl -sf "${BASE_URL}/v1/requests/${REQ_ID}" | python3 -c "import sys,json; print(json.load(sys.stdin)['state'])")
  log "  state=${STATE}"
  if [[ "$STATE" == "SUCCEEDED" || "$STATE" == "FAILED" || "$STATE" == "EXPIRED" ]]; then
    break
  fi
  sleep 1
done

if [[ "$STATE" != "SUCCEEDED" ]]; then
  log "FAIL expected SUCCEEDED got ${STATE}"
  exit 1
fi
log "single request SUCCEEDED"

log_step "batch + callback"
BATCH_BODY=$(cat <<EOF
{
  "callbackUrl": "${CALLBACK_URL}",
  "requests": [
    {"requestId":"e2e-b1-$(date +%s)","model":"model-a","estimatedTokens":50,"payload":{}},
    {"requestId":"e2e-b2-$(date +%s)","model":"model-b","estimatedTokens":50,"payload":{}}
  ]
}
EOF
)
log "POST /v1/batches (2 requests)"
BATCH_ID=$(curl -sf -X POST "${BASE_URL}/v1/batches" -H 'Content-Type: application/json' -d "$BATCH_BODY" | python3 -c "import sys,json; print(json.load(sys.stdin)['batchId'])")
log "batch accepted batchId=${BATCH_ID}"

log "polling batch until COMPLETED"
for i in $(seq 1 90); do
  STATUS=$(curl -sf "${BASE_URL}/v1/batches/${BATCH_ID}" | python3 -c "import sys,json; print(json.load(sys.stdin)['status'])")
  log "  batch status=${STATUS}"
  if [[ "$STATUS" == "COMPLETED" ]]; then
    break
  fi
  sleep 1
done

if [[ "$STATUS" != "COMPLETED" ]]; then
  log "FAIL batch not completed (status=${STATUS})"
  exit 1
fi

log "polling callback until DELIVERED"
CB_STATUS="PENDING"
for i in $(seq 1 30); do
  CB_STATUS=$(curl -sf "${BASE_URL}/v1/batches/${BATCH_ID}" | python3 -c "import sys,json; print(json.load(sys.stdin)['callbackStatus'])")
  log "  callback status=${CB_STATUS}"
  if [[ "$CB_STATUS" == "DELIVERED" ]]; then
    break
  fi
  sleep 2
done
if [[ "$CB_STATUS" != "DELIVERED" ]]; then
  log "FAIL callback status=${CB_STATUS}"
  exit 1
fi

log_step "PASS — E2E smoke test succeeded"
