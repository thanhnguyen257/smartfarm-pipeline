import requests
import time
import csv
import statistics

ES_URL = "http://192.168.1.130:9200"
INDEX = ["farm_enriched_telemetry", "farm_cleaned_alerts"]

SAMPLES = 5
INTERVAL = 10

def snapshot(index):
    url = f"{ES_URL}/{index}/_stats/indexing"
    r = requests.get(url).json()
    idx = r["_all"]["total"]["indexing"]
    return {
        "total": idx["index_total"],
        "time_ms": idx["index_time_in_millis"]
    }

for index in INDEX:
    print(f"Running ES indexing benchmark for index: {index}")
    print(f"Samples: {SAMPLES}, Interval: {INTERVAL}s\n")

    results = []

    prev = snapshot(index)
    time.sleep(1)

    for i in range(SAMPLES):
        print(f"Sample {i+1}/{SAMPLES} ... waiting {INTERVAL}s")
        time.sleep(INTERVAL)
        curr = snapshot(index)

        docs = curr["total"] - prev["total"]
        time_ms = curr["time_ms"] - prev["time_ms"]

        if time_ms > 0:
            docs_per_sec = docs / (time_ms / 1000)
        else:
            docs_per_sec = 0

        results.append({
            "index": index,
            "sample": i + 1,
            "docs": docs,
            "time_ms": time_ms,
            "docs_per_sec": docs_per_sec
        })

        print(f"  docs indexed = {docs}, time = {time_ms} ms, rate = {docs_per_sec:.2f} docs/s")

        prev = curr

    print("\n--- Summary ---")

    rates = [r["docs_per_sec"] for r in results if r['index'] == index]
    avg_rate = statistics.mean(rates)
    min_rate = min(rates)
    max_rate = max(rates)

    print(f"Average rate: {avg_rate:.2f} docs/s")
    print(f"Min rate:     {min_rate:.2f} docs/s")
    print(f"Max rate:     {max_rate:.2f} docs/s")

csv_file = "./data/es_indexing_speed.csv"
with open(csv_file, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["sample", "docs", "time_ms", "docs_per_sec"])
    for r in results:
        writer.writerow([r["sample"], r["docs"], r["time_ms"], r["docs_per_sec"]])

print(f"\nSaved: {csv_file}")
