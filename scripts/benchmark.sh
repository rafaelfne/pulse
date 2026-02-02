#!/usr/bin/env bash
# Benchmark script for Pulse Phase 1

set -e

SERVER_URL="${PULSE_URL:-http://localhost:8080}"
NUM_BATCHES="${NUM_BATCHES:-100}"
BATCH_SIZE="${BATCH_SIZE:-100}"
NUM_PARTITIONS="${NUM_PARTITIONS:-4}"

echo "=== Pulse Phase 1 Benchmark ==="
echo "Server: $SERVER_URL"
echo "Batches: $NUM_BATCHES"
echo "Batch size: $BATCH_SIZE"
echo "Total events: $((NUM_BATCHES * BATCH_SIZE))"
echo ""

# Check if server is running
if ! curl -s -f "$SERVER_URL/health" > /dev/null 2>&1; then
    echo "ERROR: Server is not running at $SERVER_URL"
    echo "Start the server with: make run"
    exit 1
fi

echo "✓ Server is running"
echo ""

# Benchmark ingestion
echo "=== Ingestion Benchmark ==="
START_TIME=$(date +%s%N)

for i in $(seq 1 $NUM_BATCHES); do
    # Generate batch payload
    EVENTS=""
    for j in $(seq 1 $BATCH_SIZE); do
        KEY="key-$((i * BATCH_SIZE + j))"
        DATA=$(echo -n "{\"batch\":$i,\"event\":$j}" | base64)
        if [ -z "$EVENTS" ]; then
            EVENTS="{\"key\":\"$KEY\",\"data\":\"$DATA\"}"
        else
            EVENTS="$EVENTS,{\"key\":\"$KEY\",\"data\":\"$DATA\"}"
        fi
    done
    
    PAYLOAD="{\"events\":[$EVENTS]}"
    
    # Send batch
    RESPONSE=$(curl -s -X POST "$SERVER_URL/events" \
        -H "Content-Type: application/json" \
        -d "$PAYLOAD")
    
    if [ $((i % 10)) -eq 0 ]; then
        echo "  Sent batch $i/$NUM_BATCHES"
    fi
done

END_TIME=$(date +%s%N)
DURATION_NS=$((END_TIME - START_TIME))
DURATION_S=$(echo "scale=3; $DURATION_NS / 1000000000" | bc)
THROUGHPUT=$(echo "scale=0; $NUM_BATCHES * $BATCH_SIZE / $DURATION_S" | bc)

echo ""
echo "Ingestion Results:"
echo "  Duration: ${DURATION_S}s"
echo "  Throughput: $THROUGHPUT events/sec"
echo ""

# Benchmark streaming
echo "=== Streaming Benchmark ==="
STREAM_START=$(date +%s%N)

TOTAL_READ=0
for p in $(seq 0 $((NUM_PARTITIONS - 1))); do
    OFFSET=0
    while true; do
        RESPONSE=$(curl -s "$SERVER_URL/stream?partition=$p&offset=$OFFSET&limit=100")
        COUNT=$(echo "$RESPONSE" | jq '.entries | length' 2>/dev/null || echo "0")
        
        if [ "$COUNT" -eq "0" ]; then
            break
        fi
        
        TOTAL_READ=$((TOTAL_READ + COUNT))
        OFFSET=$((OFFSET + COUNT))
    done
    echo "  Read from partition $p: $OFFSET events"
done

STREAM_END=$(date +%s%N)
STREAM_DURATION_NS=$((STREAM_END - STREAM_START))
STREAM_DURATION_S=$(echo "scale=3; $STREAM_DURATION_NS / 1000000000" | bc)
STREAM_THROUGHPUT=$(echo "scale=0; $TOTAL_READ / $STREAM_DURATION_S" | bc)

echo ""
echo "Streaming Results:"
echo "  Total read: $TOTAL_READ events"
echo "  Duration: ${STREAM_DURATION_S}s"
echo "  Throughput: $STREAM_THROUGHPUT events/sec"
echo ""

# Show metrics
echo "=== Server Metrics ==="
curl -s "$SERVER_URL/metrics" | jq '.'
echo ""

echo "=== Benchmark Complete ==="
