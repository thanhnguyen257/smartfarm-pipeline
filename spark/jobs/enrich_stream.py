#!/usr/bin/env python3
import json
import time
import sqlite3
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf, to_json, struct, get_json_object, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
import requests

def send_to_es(df, epoch_id, es_url, output_index="farm_enriched_telemetry"):
    """
    Send Spark microbatch to Elasticsearch via REST Bulk API.
    """
    if df.rdd.isEmpty():
        return

    rows = df.toJSON().collect()

    bulk_body = ""
    for r in rows:
        bulk_body += f'{{ "index": {{ "_index": {output_index} }} }}\n'
        bulk_body += r + "\n"

    resp = requests.post(
        f"{es_url}/_bulk",
        data=bulk_body,
        headers={"Content-Type": "application/x-ndjson"}
    )

    if resp.status_code >= 300:
        print("Bulk insert error:", resp.text)
    else:
        print("Inserted batch:", len(rows))

def load_locations(db_path):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("""
        SELECT f_gw_dv.device_id, f.id AS farm_id, f.latitude AS farm_lat, f.longitude AS farm_lon,
               g.id AS gateway_id, g.latitude AS gw_lat, g.longitude AS gw_lon,
               d.latitude AS device_lat, d.longitude AS device_lon
        FROM farm_gateway_device f_gw_dv
        JOIN farm f ON f.id = f_gw_dv.farm_id
        JOIN gateway g ON g.id = f_gw_dv.gateway_id
        JOIN device d ON d.id = f_gw_dv.device_id
    """)
    rows = cur.fetchall()
    conn.close()
    loc_map = {}
    for r in rows:
        device_id = r[0]
        loc_map[device_id] = {
            "farm_id": r[1],
            "farm_lat": r[2],
            "farm_lon": r[3],
            "gateway_id": r[4],
            "gateway_lat": r[5],
            "gateway_lon": r[6],
            "device_lat": r[7],
            "device_lon": r[8]
        }
    return loc_map
class LocationCache:
    def __init__(self, db_path, ttl_sec=300):
        self.db_path = db_path
        self.ttl_sec = ttl_sec
        self.last_load = 0
        self.cache = {}

    def get_cache(self):
        now = time.time()
        if not self.cache or (now - self.last_load) > self.ttl_sec:
            self.cache = load_locations(self.db_path)
            self.last_load = now
        return self.cache

loc_cache = None

def enrich_device(device_id):
    cache = loc_cache.get_cache()
    v = cache.get(device_id)
    if not v:
        return json.dumps({
            "farm_id": None,
            "farm_lat": None,
            "farm_lon": None,
            "gateway_id": None,
            "gateway_lat": None,
            "gateway_lon": None,
            "device_lat": None,
            "device_lon": None
        })
    return json.dumps(v)

def main(args):
    global loc_cache
    loc_cache = LocationCache(args.sqlite, ttl_sec=300)

    spark = (
        SparkSession.builder
        .appName("farm-enrichment")
        .config("spark.sql.streaming.schemaInference", "true")
        .getOrCreate()
    )

    schema = StructType([
        StructField("device_id", StringType()),
        StructField("gateway_id", StringType()),
        StructField("gateway_ts", LongType()),
        StructField("gateway_ts_iso", StringType()),
        StructField("count", LongType()),
        StructField("temperature_avg", DoubleType()),
        StructField("temperature_min", DoubleType()),
        StructField("temperature_max", DoubleType()),
        StructField("temperature_std", DoubleType()),
        StructField("humidity_avg", DoubleType()),
        StructField("humidity_min", DoubleType()),
        StructField("humidity_max", DoubleType()),
        StructField("humidity_std", DoubleType()),
        StructField("soil_moisture_avg", DoubleType()),
        StructField("soil_moisture_min", DoubleType()),
        StructField("soil_moisture_max", DoubleType()),
        StructField("soil_moisture_std", DoubleType()),
        StructField("light_level_avg", DoubleType()),
        StructField("light_level_min", DoubleType()),
        StructField("light_level_max", DoubleType()),
        StructField("light_level_std", DoubleType())
    ])

    kafka_df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", args.kafka) \
        .option("subscribe", args.input_topic) \
        .option("startingOffsets", "latest") \
        .load()

    json_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str")
    parsed_df = json_df.select(from_json(col("json_str"), schema).alias("d")).select("d.*")

    enrich_udf = udf(enrich_device, StringType())
    enriched_df = parsed_df.withColumn("loc_json", enrich_udf(col("device_id")))

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

    # kafka_out_df = final_df.select(to_json(struct([col(c) for c in final_df.columns])).alias("value"))

    # kafka_query = kafka_out_df.writeStream \
    #     .format("kafka") \
    #     .option("kafka.bootstrap.servers", args.kafka) \
    #     .option("topic", args.output_topic) \
    #     .option("checkpointLocation", "/tmp/spark_checkpoint/farm_enrich") \
    #     .outputMode("append") \
    #     .start()

    # es_df = final_df.withColumn("_index", lit("farm_enriched_telemetry"))
    es_query = final_df.writeStream \
        .outputMode("append") \
        .foreachBatch(lambda df, epoch_id: send_to_es(df, epoch_id, args.es, args.output_topic)) \
        .option("checkpointLocation", "/tmp/spark_checkpoint/farm_enrich_es_http") \
        .start()
    
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--kafka", default="broker1:29092", help="Kafka bootstrap servers")
    parser.add_argument("--input_topic", default="farm_raw_telemetry", help="Kafka input topic")
    parser.add_argument("--output_topic", default="farm_enrich_telemetry", help="Kafka output topic")
    parser.add_argument("--sqlite", default="/opt/locations.db", help="SQLite DB path for enrichment")
    parser.add_argument("--es", default="http://elasticsearch:9200", help="Elasticsearch endpoint")
    args = parser.parse_args()
    main(args)
