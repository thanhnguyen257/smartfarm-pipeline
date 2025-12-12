#!/usr/bin/env python3
import requests
import pandas as pd
import numpy as np

ES_BASE = "http://192.168.1.130:9200"
INDEX = ["farm_enriched_telemetry","farm_cleaned_alerts"]
BATCH_SIZE = 5000
TIME_RANGE = "now-6h"

ES_AUTH = None

def es_search(index):
    url = f"{ES_BASE}/{index}/_search?scroll=1m"
    query = {
        "size": BATCH_SIZE,
        "query": {
            "range": {
                "created_at": {
                    "gte": TIME_RANGE
                }
            }
        }
    }
    return requests.post(url, json=query, auth=ES_AUTH).json()


def es_scroll(scroll_id):
    url = f"{ES_BASE}/_search/scroll"
    body = {
        "scroll": "1m",
        "scroll_id": scroll_id
    }
    return requests.post(url, json=body, auth=ES_AUTH).json()


def parse_hits(hits):
    rows = []
    for h in hits:
        s = h["_source"]
        try:
            rows.append({
                "gateway_ts": pd.to_datetime(s["gateway_ts"], unit="ms"),
                "ts_kafka_ingest": pd.to_datetime(s["ts_kafka_ingest"], unit="ms"),
                "ts_before_es": pd.to_datetime(s["ts_before_es"], unit="ms"),
                "created_at": pd.to_datetime(s["created_at"])
            })
        except Exception as e:
            print("⚠️ Skipped bad record:", e)
            continue
    return rows


def evaluate_latency(df):
    df["created_at"] = df["created_at"].dt.tz_convert("UTC").dt.tz_localize(None)
    df["gateway_ts"] = df["gateway_ts"].dt.tz_localize("UTC").dt.tz_localize(None)
    df["ts_kafka_ingest"] = df["ts_kafka_ingest"].dt.tz_localize("UTC").dt.tz_localize(None)
    df["ts_before_es"] = df["ts_before_es"].dt.tz_localize("UTC").dt.tz_localize(None)

    df["lat_gw_kafka"] = (
        df["ts_kafka_ingest"] - df["gateway_ts"]
    ).dt.total_seconds()

    df["lat_kafka_spark"] = (
        df["ts_before_es"] - df["ts_kafka_ingest"]
    ).dt.total_seconds()

    df["lat_spark_es"] = (
        df["created_at"] - df["ts_before_es"]
    ).dt.total_seconds()

    df["lat_e2e"] = (
        df["created_at"] - df["gateway_ts"]
    ).dt.total_seconds()

    return df


def print_stats(label, values):
    print(f"\n==== {label} ====")
    print(f"Count: {len(values)}")
    print(f"Avg: {np.mean(values):.3f}s")
    print(f"P50: {np.percentile(values, 50):.3f}s")
    print(f"P90: {np.percentile(values, 90):.3f}s")
    print(f"P95: {np.percentile(values, 95):.3f}s")
    print(f"Min: {np.min(values):.3f}s")
    print(f"Max: {np.max(values):.3f}s")


def main():
    print("Fetching data from Elasticsearch...")
    df_all = pd.DataFrame()
    for index in INDEX:
        print("\n==============================")
        print(f"Index: {index}")
        rows = []
        resp = es_search(index)
        scroll_id = resp.get("_scroll_id")
        hits = resp["hits"]["hits"]
        rows.extend(parse_hits(hits))

        while hits:
            resp = es_scroll(scroll_id)
            hits = resp["hits"]["hits"]
            if not hits:
                break
            scroll_id = resp.get("_scroll_id")
            rows.extend(parse_hits(hits))

        df = pd.DataFrame(rows)
        if df.empty:
            print("No data found in ES.")
            return

        df = evaluate_latency(df)

        print_stats("Gateway → Kafka", df["lat_gw_kafka"])
        print_stats("Kafka → Spark", df["lat_kafka_spark"])
        print_stats("Spark → ES", df["lat_spark_es"])
        print_stats("End-to-End", df["lat_e2e"])
        df["index"] = index
        df_all = pd.concat([df_all, df], ignore_index=True)

    df_all.to_csv("./data/latency_results.csv", index=False)
    print("Saved to latency_results.csv")


if __name__ == "__main__":
    main()
