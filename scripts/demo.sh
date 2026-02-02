#!/usr/bin/env bash
# Quick demo of Pulse Phase 1 functionality

set -e

SERVER_URL="${PULSE_URL:-http://localhost:8080}"

echo "╔════════════════════════════════════════════════════╗"
echo "║         Pulse Phase 1 - Quick Demo                ║"
echo "╚════════════════════════════════════════════════════╝"
echo ""

# Check server
echo "→ Checking server health..."
if ! curl -s -f "$SERVER_URL/health" > /dev/null 2>&1; then
    echo "✗ Server is not running at $SERVER_URL"
    echo ""
    echo "  Start the server with: make run"
    exit 1
fi
echo "✓ Server is healthy"
echo ""

# Demo 1: Ingest events
echo "╔════════════════════════════════════════════════════╗"
echo "║  Demo 1: Ingesting Events                         ║"
echo "╚════════════════════════════════════════════════════╝"
echo ""
echo "→ Ingesting 5 events with different keys..."

curl -s -X POST "$SERVER_URL/events" \
  -H "Content-Type: application/json" \
  -d '{
    "events": [
      {"key": "user-alice", "data": "eyJhY3Rpb24iOiJsb2dpbiIsInRpbWVzdGFtcCI6MTczODE2MDAwMH0="},
      {"key": "user-bob", "data": "eyJhY3Rpb24iOiJzaWdudXAiLCJ0aW1lc3RhbXAiOjE3MzgxNjAwMTB9"},
      {"key": "user-alice", "data": "eyJhY3Rpb24iOiJ2aWV3X3Byb2ZpbGUiLCJ0aW1lc3RhbXAiOjE3MzgxNjAwMjB9"},
      {"key": "user-charlie", "data": "eyJhY3Rpb24iOiJwb3N0X21lc3NhZ2UiLCJ0aW1lc3RhbXAiOjE3MzgxNjAwMzB9"},
      {"key": "user-alice", "data": "eyJhY3Rpb24iOiJsb2dvdXQiLCJ0aW1lc3RhbXAiOjE3MzgxNjAwNDB9"}
    ]
  }' | jq '.'

echo ""
echo "✓ Events ingested and assigned to partitions"
echo ""

# Wait for flush
sleep 1

# Demo 2: Stream events
echo "╔════════════════════════════════════════════════════╗"
echo "║  Demo 2: Streaming Events by Partition            ║"
echo "╚════════════════════════════════════════════════════╝"
echo ""

for p in 0 1 2 3; do
  RESPONSE=$(curl -s "$SERVER_URL/stream?partition=$p&offset=0&limit=10")
  COUNT=$(echo "$RESPONSE" | jq '.entries | length')
  
  if [ "$COUNT" -gt 0 ]; then
    echo "→ Partition $p contains $COUNT event(s):"
    echo "$RESPONSE" | jq -r '.entries[] | "  • Offset \(.Offset): key=\"\(.Key)\""'
    echo ""
  fi
done

# Demo 3: Streaming with offset
echo "╔════════════════════════════════════════════════════╗"
echo "║  Demo 3: Offset-based Streaming                   ║"
echo "╚════════════════════════════════════════════════════╝"
echo ""
echo "→ Reading from partition 3, starting at offset 1..."
curl -s "$SERVER_URL/stream?partition=3&offset=1&limit=10" | jq '.'
echo ""

# Demo 4: Metrics
echo "╔════════════════════════════════════════════════════╗"
echo "║  Demo 4: Server Metrics                           ║"
echo "╚════════════════════════════════════════════════════╝"
echo ""
echo "→ Current server metrics:"
curl -s "$SERVER_URL/metrics" | jq '.'
echo ""

# Demo 5: Key-based ordering
echo "╔════════════════════════════════════════════════════╗"
echo "║  Demo 5: Same-key Ordering Guarantee              ║"
echo "╚════════════════════════════════════════════════════╝"
echo ""
echo "→ All 'user-alice' events (same key, same partition):"

for p in 0 1 2 3; do
  RESPONSE=$(curl -s "$SERVER_URL/stream?partition=$p&offset=0&limit=100")
  ALICE_EVENTS=$(echo "$RESPONSE" | jq -r '.entries[] | select(.Key == "user-alice") | "  Offset \(.Offset) @ partition '$p'"')
  
  if [ -n "$ALICE_EVENTS" ]; then
    echo "$ALICE_EVENTS"
  fi
done

echo ""
echo "✓ Events with the same key are ordered within their partition"
echo ""

echo "╔════════════════════════════════════════════════════╗"
echo "║  Demo Complete!                                    ║"
echo "╚════════════════════════════════════════════════════╝"
echo ""
echo "Key Takeaways:"
echo "  • Events are distributed across partitions by key hash"
echo "  • Events with the same key go to the same partition"
echo "  • Offsets are monotonically increasing per partition"
echo "  • Data persists across restarts"
echo "  • Metrics track throughput and latency"
echo ""
echo "Next: Try the benchmark with './scripts/benchmark.sh'"
