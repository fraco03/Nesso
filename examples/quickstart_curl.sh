#!/usr/bin/env bash
set -e

# ==============================================================================
# Nesso HTTP Quickstart & Demo Script
# ==============================================================================

PORT=8080
BASE_URL="http://127.0.0.1:${PORT}"
QUEUE="notifications"

echo "=== 1. Health Check ==="
curl -s "${BASE_URL}/health"
echo -e "\n"

echo "=== 2. Push Task (High Priority: 10) ==="
PAYLOAD_B64=$(echo -n '{"email": "user@example.com", "template": "welcome"}' | base64)
PUSH_RESP=$(curl -s -X POST "${BASE_URL}/v1/queues/${QUEUE}/push?sync=true" \
  -H "Content-Type: application/json" \
  -d "{\"payload\": \"${PAYLOAD_B64}\", \"priority\": 10}")
echo "Response: ${PUSH_RESP}"
TASK_ID=$(echo "${PUSH_RESP}" | grep -o '"id":[0-9]*' | cut -d':' -f2)

echo -e "\n=== 3. Queue Status ==="
curl -s "${BASE_URL}/v1/queues/${QUEUE}/status"
echo -e "\n"

echo "=== 4. Pop & Lease Task (Consumer ID: 42, Lease: 30s) ==="
POP_RESP=$(curl -s -X POST "${BASE_URL}/v1/queues/${QUEUE}/pop" \
  -H "Content-Type: application/json" \
  -d '{"consumer_id": 42, "lease_secs": 30, "wait_secs": 2}')
echo "Response: ${POP_RESP}"

echo -e "\n=== 5. Ack Task #${TASK_ID} ==="
curl -s -X POST "${BASE_URL}/v1/queues/${QUEUE}/tasks/${TASK_ID}/ack?sync=true" \
  -H "Content-Type: application/json" \
  -d '{"consumer_id": 42}'
echo "Task Acked successfully."

echo -e "\n=== 6. Trigger WAL Compaction ==="
COMPACT_RESP=$(curl -s -X POST "${BASE_URL}/v1/queues/${QUEUE}/compact")
echo "Compact Response: ${COMPACT_RESP}"

echo -e "\n=== 7. Final Queue Status ==="
curl -s "${BASE_URL}/v1/queues/${QUEUE}/status"
echo -e "\n"

echo "=== Demo Completed Successfully ==="
