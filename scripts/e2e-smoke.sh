#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BASE_URL="${BASE_URL:-http://localhost:8080}"
CALLBACK_URL="${CALLBACK_URL:-http://callback-mock:9000/callback}"

echo "==> E2E smoke test against ${BASE_URL}"

wait_for() {
  local url=$1
  local name=$2
  for i in $(seq 1 60); do
    if curl -sf "$url" >/dev/null 2>&1; then
      echo "    ${name} ready"
      return 0
    fi
    sleep 2
  done
  echo "    TIMEOUT waiting for ${name}"
  return 1
}

wait_for "${BASE_URL}/actuator/health" "ingest-api"
wait_for "http://localhost:9000/health" "callback-mock"

REQ_ID="e2e-req-$(date +%s)"
echo "==> Submit single request ${REQ_ID}"
curl -sf -X POST "${BASE_URL}/v1/inference" \
  -H 'Content-Type: application/json' \
  -d "{\"requestId\":\"${REQ_ID}\",\"model\":\"model-a\",\"estimatedTokens\":100,\"payload\":{\"prompt\":\"hi\"}}" \
  | tee /tmp/e2e-single.json

echo "==> Poll until terminal"
for i in $(seq 1 60); do
  STATE=$(curl -sf "${BASE_URL}/v1/requests/${REQ_ID}" | python3 -c "import sys,json; print(json.load(sys.stdin)['state'])")
  echo "    state=${STATE}"
  if [[ "$STATE" == "SUCCEEDED" || "$STATE" == "FAILED" || "$STATE" == "EXPIRED" ]]; then
    break
  fi
  sleep 1
done

if [[ "$STATE" != "SUCCEEDED" ]]; then
  echo "FAIL: expected SUCCEEDED, got ${STATE}"
  exit 1
fi

echo "==> Submit batch with callback"
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
BATCH_ID=$(curl -sf -X POST "${BASE_URL}/v1/batches" -H 'Content-Type: application/json' -d "$BATCH_BODY" | python3 -c "import sys,json; print(json.load(sys.stdin)['batchId'])")
echo "    batchId=${BATCH_ID}"

echo "==> Wait for batch completion"
for i in $(seq 1 90); do
  STATUS=$(curl -sf "${BASE_URL}/v1/batches/${BATCH_ID}" | python3 -c "import sys,json; print(json.load(sys.stdin)['status'])")
  echo "    batch status=${STATUS}"
  if [[ "$STATUS" == "COMPLETED" ]]; then
    break
  fi
  sleep 1
done

if [[ "$STATUS" != "COMPLETED" ]]; then
  echo "FAIL: batch not completed"
  exit 1
fi

echo "==> Verify callback delivered"
CB_STATUS="PENDING"
for i in $(seq 1 30); do
  CB_STATUS=$(curl -sf "${BASE_URL}/v1/batches/${BATCH_ID}" | python3 -c "import sys,json; print(json.load(sys.stdin)['callbackStatus'])")
  echo "    callback status=${CB_STATUS}"
  if [[ "$CB_STATUS" == "DELIVERED" ]]; then
    break
  fi
  sleep 2
done
if [[ "$CB_STATUS" != "DELIVERED" ]]; then
  echo "FAIL: callback status=${CB_STATUS}"
  exit 1
fi

echo "PASS: E2E smoke test succeeded"
