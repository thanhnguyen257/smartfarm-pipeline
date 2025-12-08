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
  },
  "mappings": {
    "properties": {
      "device_id": { "type": "keyword" },
      "gateway_id": { "type": "keyword" },
      "farm_id": { "type": "keyword" },
      "gateway_ts": { "type": "long" },
      "gateway_ts_iso": { "type": "date" },
      "count": { "type": "integer" },
      "temperature_avg": { "type": "float" },
      "temperature_min": { "type": "float" },
      "temperature_max": { "type": "float" },
      "temperature_std": { "type": "float" },
      "humidity_avg": { "type": "float" },
      "humidity_min": { "type": "float" },
      "humidity_max": { "type": "float" },
      "humidity_std": { "type": "float" },
      "soil_moisture_avg": { "type": "float" },
      "soil_moisture_min": { "type": "float" },
      "soil_moisture_max": { "type": "float" },
      "soil_moisture_std": { "type": "float" },
      "light_level_avg": { "type": "float" },
      "light_level_min": { "type": "float" },
      "light_level_max": { "type": "float" },
      "light_level_std": { "type": "float" },
      "device_location": { "type": "geo_point" },
      "gateway_location": { "type": "geo_point" },
      "farm_location": { "type": "geo_point" }
    }
  }
}'

FARM_CLEANED_ALERTS_SETTINGS='{
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 0
  },
  "mappings": {
    "properties": {
      "device_id": { "type": "keyword" },
      "gateway_id": { "type": "keyword" },
      "farm_id": { "type": "keyword" },
      "gateway_ts": { "type": "long" },
      "gateway_ts_iso": { "type": "date" },
      "state": { "type": "keyword" },
      "details": { "type": "object" },
      "device_location": { "type": "geo_point" },
      "gateway_location": { "type": "geo_point" },
      "farm_location": { "type": "geo_point" }
    }
  }
}'

create_index "farm_enriched_telemetry" "$FARM_ENRICHED_TELEMETRY_SETTINGS"
create_index "farm_cleaned_alerts" "$FARM_CLEANED_ALERTS_SETTINGS"

echo "All Elasticsearch indices ready ✅"
