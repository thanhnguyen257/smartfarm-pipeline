#!/bin/sh
set -e

ES_HOST="elasticsearch"
ES_PORT=9200

echo "Waiting for Elasticsearch to be ready..."
until curl -s http://$ES_HOST:$ES_PORT/_cluster/health | grep '"status"' >/dev/null; do
    sleep 2
done
echo "Elasticsearch is up ✅"

create_index() {
    INDEX_NAME=$1
    SETTINGS=$2

    if curl -s -o /dev/null -w "%{http_code}" http://$ES_HOST:$ES_PORT/$INDEX_NAME | grep -q "404"; then
        echo "Creating index: $INDEX_NAME"
        curl -s -X PUT "http://$ES_HOST:$ES_PORT/$INDEX_NAME" \
            -H 'Content-Type: application/json' \
            -d "$SETTINGS"
        echo "Index $INDEX_NAME created ✅"
    else
        echo "Index $INDEX_NAME already exists, skipping."
    fi
}


FARM_ENRICHED_TELEMETRY_SETTINGS='{
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 0
  }
}'

FARM_ALERTS_CRITICAL_SETTINGS='{
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 0
  }
}'

create_index "farm_enriched_telemetry" "$FARM_ENRICHED_TELEMETRY_SETTINGS"
create_index "farm_alerts_critical" "$FARM_ALERTS_CRITICAL_SETTINGS"

echo "All Elasticsearch indices ready ✅"
