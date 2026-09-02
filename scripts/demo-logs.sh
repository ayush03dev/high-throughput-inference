#!/usr/bin/env bash
# Tail demo-friendly logs for Loom recording (services + tagged script output).
set -euo pipefail

cd "$(dirname "$0")/.."

echo "[demo-logs] Following service logs: inference-gateway, request-processor, webhook-delivery, provider-mock, webhook-mock"
echo "[demo-logs] Service tags: [gateway] [processor] [batch] [limits] [webhook] [provider] [webhook-mock]"
echo "[demo-logs] Script tags:  [loadgen] [validate] [e2e] — run those in another terminal"
echo ""

docker compose logs -f --tail=30 \
  inference-gateway \
  request-processor \
  webhook-delivery \
  provider-mock \
  webhook-mock
