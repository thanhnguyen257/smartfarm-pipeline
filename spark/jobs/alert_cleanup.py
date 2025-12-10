#!/usr/bin/env python3
import json
import argparse
import sqlite3
import time
import requests
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf, struct, to_json, get_json_object
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, LongType


RAW_LOG_DIR = "/opt/alert_logs"
RAW_LOG_WINDOW = 300
_raw_log_last = 0
_raw_log_file = None

def now_millis():
    return int(time.time() * 1000)

def raw_log_write(lines):
    """
    Write raw kafka messages to rotating log files.
    Log rotates every RAW_LOG_WINDOW seconds.
    """
    global _raw_log_last, _raw_log_file
    now = int(time.time())

    if (_raw_log_file is None) or (now - _raw_log_last > RAW_LOG_WINDOW):
        if _raw_log_file:
            _raw_log_file.close()
        fname = os.path.join(RAW_LOG_DIR, f"raw_alert_{now}.log")
        _raw_log_file = open(fname, "a")
        _raw_log_last = now

    for line in lines:
        _raw_log_file.write(line + "\n")
    _raw_log_file.flush()

def load_locations(db_path):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("""
        SELECT f_gw_dv.device_id,
               f.id AS farm_id, f.latitude AS farm_lat, f.longitude AS farm_lon,
               g.id AS gateway_id, g.latitude AS gw_lat, g.longitude AS gw_lon,
               d.latitude AS device_lat, d.longitude AS device_lon
        FROM farm_gateway_device f_gw_dv
        JOIN farm f ON f.id = f_gw_dv.farm_id
        JOIN gateway g ON g.id = f_gw_dv.gateway_id
        JOIN device d ON d.id = f_gw_dv.device_id
    """)
    rows = cur.fetchall()
    conn.close()

    out = {}
    for r in rows:
        device_id = r[0]
        out[device_id] = {
            "farm_id": r[1],
            "farm_lat": r[2],
            "farm_lon": r[3],
            "gateway_lat": r[5],
            "gateway_lon": r[6],
            "device_lat": r[7],
            "device_lon": r[8]
        }
    return out


class LocCache:
    def __init__(self, db_path, ttl=300):
        self.db_path = db_path
        self.ttl = ttl
        self.last = 0
        self.cache = {}

    def get(self):
        now = time.time()
        if (now - self.last) > self.ttl:
            self.cache = load_locations(self.db_path)
            self.last = now
        return self.cache


loc_cache = None


def enrich_loc(device_id):
    m = loc_cache.get()
    return json.dumps(m.get(device_id, {
        "farm_id": None,
        "farm_lat": None,
        "farm_lon": None,
        "gateway_lat": None,
        "gateway_lon": None,
        "device_lat": None,
        "device_lon": None
    }))


last_alert_state = {}
DEDUP_WINDOW = 30


def dedup_alert(device_id, state, ts):
    prev = last_alert_state.get(device_id)
    if prev:
        prev_state, prev_ts = prev
        if prev_state == state and abs(ts - prev_ts) < DEDUP_WINDOW:
            return "DROP"

    last_alert_state[device_id] = (state, ts)
    return "KEEP"


def main(args):
    global loc_cache, RAW_LOG_WINDOW

    RAW_LOG_WINDOW = args.raw_log_window
    RAW_LOG_DIR = args.raw_log_dir

    os.makedirs(RAW_LOG_DIR, exist_ok=True)

    loc_cache = LocCache(args.sqlite)

    spark = (
        SparkSession.builder
        .appName("alert-cleanup")
        .config("spark.sql.streaming.schemaInference", "true")
        .getOrCreate()
    )

    schema = StructType([
        StructField("device_id", StringType()),
        StructField("gateway_id", StringType()),
        StructField("gateway_ts", LongType()),
        StructField("gateway_ts_iso", StringType()),
        StructField("state", StringType()),
        StructField("details", ArrayType(StringType()))
    ])

    kafka_df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", args.kafka) \
        .option("subscribe", args.input_topic) \
        .option("startingOffsets", "latest") \
        .load()

    json_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str")
    raw_df = json_df.select(from_json(col("json_str"), schema).alias("a")).select("a.*")

    now_millis_udf = udf(now_millis, LongType())
    raw_df = raw_df.withColumn("ts_kafka_ingest", now_millis_udf())

    loc_udf = udf(enrich_loc, StringType())
    enriched_df = raw_df.withColumn("loc_json", loc_udf(col("device_id")))

    final_df = enriched_df \
        .withColumn("farm_id", get_json_object(col("loc_json"), "$.farm_id")) \
        .withColumn("farm_lat", get_json_object(col("loc_json"), "$.farm_lat")) \
        .withColumn("farm_lon", get_json_object(col("loc_json"), "$.farm_lon")) \
        .withColumn("gateway_lat", get_json_object(col("loc_json"), "$.gateway_lat")) \
        .withColumn("gateway_lon", get_json_object(col("loc_json"), "$.gateway_lon")) \
        .withColumn("device_lat", get_json_object(col("loc_json"), "$.device_lat")) \
        .withColumn("device_lon", get_json_object(col("loc_json"), "$.device_lon")) \
        .withColumn("farm_location", struct(col("farm_lat").cast("double").alias("lat"),
                                            col("farm_lon").cast("double").alias("lon"))) \
        .withColumn("gateway_location", struct(col("gateway_lat").cast("double").alias("lat"),
                                            col("gateway_lon").cast("double").alias("lon"))) \
        .withColumn("device_location", struct(col("device_lat").cast("double").alias("lat"),
                                            col("device_lon").cast("double").alias("lon"))) \
        .drop("loc_json", "farm_lat", "farm_lon", "gateway_lat", "gateway_lon", "device_lat", "device_lon")

    def process_alerts(df, epoch_id):
        if df.rdd.isEmpty():
            return

        raw_lines = df.toJSON().collect()
        raw_log_write(raw_lines)

        alerts = raw_lines
        cleaned = []

        for a in alerts:
            obj = json.loads(a)
            if dedup_alert(obj["device_id"], obj["state"], obj["gateway_ts"]) == "KEEP":
                obj["ts_before_es"] = now_millis()
                cleaned.append(obj)

        if not cleaned:
            return

        bulk = ""
        for c in cleaned:
            bulk += f'{{ "index": {{ "_index": "{args.es_index}" }} }}\n'
            bulk += json.dumps(c) + "\n"

        requests.post(
            f"{args.es}/_bulk",
            headers={"Content-Type": "application/x-ndjson"},
            data=bulk
        )

        kafka_out = (
            spark.createDataFrame(cleaned)
            .select(to_json(struct("*")).alias("value"))
        )

        kafka_out.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", args.kafka) \
            .option("topic", args.output_topic) \
            .save()

    query = final_df.writeStream \
        .outputMode("append") \
        .foreachBatch(process_alerts) \
        .option("checkpointLocation", "/tmp/spark_checkpoint/alert_cleanup") \
        .trigger(processingTime="10 seconds") \
        .start()

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--kafka", default="kafka-broker:29092")
    parser.add_argument("--input_topic", default="farm_raw_alerts")
    parser.add_argument("--output_topic", default="farm_cleaned_alerts")
    parser.add_argument("--sqlite", default="/opt/locations.db")
    parser.add_argument("--es", default="http://elasticsearch:9200")
    parser.add_argument("--es_index", default="farm_cleaned_alerts")
    parser.add_argument("--raw_log_window", type=int, default=300)
    parser.add_argument("--raw_log_dir", default="/opt/alert_logs")
    args = parser.parse_args()
    main(args)
