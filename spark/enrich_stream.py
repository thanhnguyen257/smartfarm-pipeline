import json
import argparse
import sqlite3
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

def load_locations(sqlite_path):
    conn = sqlite3.connect(sqlite_path)
    cur = conn.cursor()
    cur.execute("SELECT device_id, latitude, longitude, gateway_id, farm_id FROM devices")
    rows = cur.fetchall()
    conn.close()
    loc_map = {r[0]: {"latitude": r[1], "longitude": r[2], "gateway_id": r[3], "farm_id": r[4]} for r in rows}
    return loc_map

def main(args):
    loc_map = load_locations(args.sqlite)
    spark = SparkSession.builder \
        .appName("iot-enrich") \
        .getOrCreate()

    # Kafka value expected as JSON string (telemetry records)
    kafka_df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", args.kafka) \
        .option("subscribe", args.topic) \
        .option("startingOffsets", "latest") \
        .load()

    schema = StructType([
        StructField("farm_id", StringType()),
        StructField("gateway_id", StringType()),
        StructField("device_id", StringType()),
        StructField("ts", LongType()),
        StructField("ts_iso", StringType()),
        StructField("temperature", DoubleType()),
        StructField("humidity", DoubleType()),
        StructField("soil_moisture", DoubleType()),
        StructField("light_level", DoubleType()),
        StructField("device_status", StringType()),
    ])

    json_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str")
    parsed = json_df.select(from_json(col("json_str"), schema).alias("d")).select("d.*")

    # UDF enrichment: attach lat/lon from loc_map
    def enrich(device_id):
        v = loc_map.get(device_id)
        if not v:
            return json.dumps({"latitude": None, "longitude": None, "gateway_id": None, "farm_id": None})
        return json.dumps(v)
    enrich_udf = udf(enrich, StringType())

    enriched = parsed.withColumn("loc_json", enrich_udf(col("device_id")))

    def send_to_es(batch_df, batch_id):
        docs = [row.asDict() for row in batch_df.collect()]
        if not docs:
            return
        # transform each row: make ES doc
        for d in docs:
            try:
                es_doc = {
                    "farm_id": d.get("farm_id"),
                    "gateway_id": d.get("gateway_id"),
                    "device_id": d.get("device_id"),
                    "ts": d.get("ts"),
                    "ts_iso": d.get("ts_iso"),
                    "temperature": d.get("temperature"),
                    "humidity": d.get("humidity"),
                    "soil_moisture": d.get("soil_moisture"),
                    "light_level": d.get("light_level"),
                    "device_status": d.get("device_status"),
                }
                loc = json.loads(d.get("loc_json") or "{}")
                es_doc.update({"location": {"lat": loc.get("latitude"), "lon": loc.get("longitude")}, "gateway_id": loc.get("gateway_id")})
                # index into ES
                resp = requests.post(f"{args.es}/farm_enriched_telemetry/_doc", json=es_doc, timeout=5)
                if not (200 <= resp.status_code < 300):
                    print("ES indexing failed", resp.status_code, resp.text)
            except Exception as e:
                print("Exception indexing doc:", e)

    query = enriched.writeStream \
        .foreachBatch(send_to_es) \
        .outputMode("append") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--kafka", default="broker1:29092")
    parser.add_argument("--topic", default="farm_raw_telemetry")
    parser.add_argument("--sqlite", default="/opt/locations.db")
    parser.add_argument("--es", default="http://elasticsearch:9200")
    args = parser.parse_args()
    main(args)
