#!/usr/bin/env python3
import json
import sqlite3
import argparse
from kafka import KafkaConsumer


def load_recipients(db_path):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT farm_id, email FROM alert_routes")
    rows = cur.fetchall()
    conn.close()

    mapping = {}
    for farm_id, email in rows:
        mapping.setdefault(str(farm_id), []).append(email)
    return mapping


def main(args):
    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.kafka,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )

    print("Notifier started... waiting for alerts")

    notify_map = load_recipients(args.sqlite)

    for msg in consumer:
        alert = msg.value
        farm_id = str(alert.get("farm_id"))
        dev_id = alert.get("device_id")
        state = alert.get("state")
        device_location = alert.get("device_location", {})
        lat = device_location.get("lat")
        lon = device_location.get("lon")

        emails = notify_map.get(farm_id, [])
        if not emails:
            print(f"[NO RECIPIENT] farm_id={farm_id}, alert={alert}")
            continue

        for email in emails:
            print(f"""
=== ALERT NOTIFICATION ===
To: {email}
Device: {dev_id}
State: {state}
Location: ({lat}, {lon})
Timestamp: {alert.get("gateway_ts_iso")}
Details: {alert.get("details")}
==========================
""")

    consumer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--kafka", default="broker1:29092")
    parser.add_argument("--sqlite", default="/app/alert_routes.db")
    parser.add_argument("--topic", default="farm_cleaned_alerts")
    args = parser.parse_args()

    main(args)
