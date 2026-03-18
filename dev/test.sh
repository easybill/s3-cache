#!/usr/bin/env bash
set -euo pipefail

S3_ENDPOINT="http://s3-cache:8080"

s3_call() {
  aws --endpoint-url "${S3_ENDPOINT}" "$@"
}

echo "=== Waiting for s3-cache to be healthy ==="
until curl -sf "${S3_ENDPOINT}/health" > /dev/null 2>&1; do
  sleep 1
done
echo "s3-cache is ready"

echo ""
echo "=== Creating test bucket ==="
s3_call s3 mb s3://test-bucket

echo ""
echo "=== Generating test files ==="
dd if=/dev/urandom of=/tmp/small.bin bs=1K count=10 2>/dev/null
dd if=/dev/urandom of=/tmp/medium.bin bs=1K count=100 2>/dev/null
dd if=/dev/urandom of=/tmp/large.bin bs=1M count=5 2>/dev/null
dd if=/dev/urandom of=/tmp/oversized.bin bs=1M count=15 2>/dev/null

echo ""
echo "=== Uploading objects ==="
s3_call s3 cp /tmp/small.bin s3://test-bucket/small.bin
s3_call s3 cp /tmp/medium.bin s3://test-bucket/medium.bin
s3_call s3 cp /tmp/large.bin s3://test-bucket/large.bin

echo ""
echo "=== GET objects (cache misses) ==="
s3_call s3 cp s3://test-bucket/small.bin /tmp/dl-small.bin
s3_call s3 cp s3://test-bucket/medium.bin /tmp/dl-medium.bin
s3_call s3 cp s3://test-bucket/large.bin /tmp/dl-large.bin

echo ""
echo "=== GET objects again (cache hits) ==="
s3_call s3 cp s3://test-bucket/small.bin /tmp/dl-small.bin
s3_call s3 cp s3://test-bucket/medium.bin /tmp/dl-medium.bin
s3_call s3 cp s3://test-bucket/large.bin /tmp/dl-large.bin

echo ""
echo "=== Upload oversized object (>10MB, should not be cached) ==="
s3_call s3 cp /tmp/oversized.bin s3://test-bucket/oversized.bin
s3_call s3 cp s3://test-bucket/oversized.bin /tmp/dl-oversized.bin
s3_call s3 cp s3://test-bucket/oversized.bin /tmp/dl-oversized.bin

echo ""
echo "=== PUT overwrite to trigger cache invalidation ==="
s3_call s3 cp /tmp/small.bin s3://test-bucket/medium.bin
s3_call s3 cp s3://test-bucket/medium.bin /tmp/dl-medium.bin

echo ""
echo "=== DELETE object ==="
s3_call s3 rm s3://test-bucket/small.bin

echo ""
echo "=== GET deleted object (expect error) ==="
s3_call s3 cp s3://test-bucket/small.bin /tmp/dl-small.bin 2>&1 || true

echo ""
echo "=== GET non-existent object (expect error) ==="
s3_call s3 cp s3://test-bucket/nonexistent.bin /tmp/dl-none.bin 2>&1 || true

echo ""
echo "=== List objects ==="
s3_call s3 ls s3://test-bucket/

echo ""
echo "=== Test script completed ==="
echo "Metrics will be written within ~10s to /metrics/s3_cache.prom"
echo "  docker compose exec s3-cache cat /metrics/s3_cache.prom"
echo "  docker compose logs s3-cache"
