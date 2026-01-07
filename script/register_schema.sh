#!/bin/bash
# Register Avro schemas to Schema Registry

set -e

REGISTRY_URL="${SCHEMA_REGISTRY_URL:-http://schema-registry:8081}"
SCHEMA_DIR="/app/schema"

register_schema() {
    local topic=$1
    local schema_file=$2
    local subject="${topic}-value"
    
    echo "Registering schema for ${subject}..."
    
    # Convert schema file to JSON string for API
    schema=$(cat "${SCHEMA_DIR}/${schema_file}" | tr -d '\n' | sed 's/"/\\"/g')
    
    response=$(curl -s -w "\n%{http_code}" -X POST "${REGISTRY_URL}/subjects/${subject}/versions" \
        -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        -d "{\"schema\": \"${schema}\"}")
    
    http_code=$(echo "$response" | tail -n1)
    body=$(echo "$response" | head -n-1)
    
    if [ "$http_code" -eq 200 ] || [ "$http_code" -eq 409 ]; then
        echo "✓ ${subject} registered (ID: ${body})"
    else
        echo "✗ Failed to register ${subject}: ${body}"
        exit 1
    fi
}

echo "Waiting for Schema Registry at ${REGISTRY_URL}..."
until curl -s "${REGISTRY_URL}/subjects" > /dev/null 2>&1; do
    echo "  Schema Registry not ready, retrying in 2s..."
    sleep 2
done
echo "Schema Registry is ready!"

echo ""
echo "Registering schemas..."
register_schema "raw_trades" "trade.avsc"
register_schema "raw_tickers" "ticker.avsc"
register_schema "alerts" "alert.avsc"

echo ""
echo "All schemas registered successfully!"
