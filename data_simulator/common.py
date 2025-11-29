from dataclasses import dataclass, asdict
from typing import Dict, Any, Optional, List
import threading
import os
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timezone
import sqlite3

@dataclass
class Telemetry:
    device_id: str
    device_ts: float
    temperature: float
    humidity: float
    soil_moisture: float
    light_level: float
    device_status: str

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d['device_ts'] = int(self.device_ts * 1000)
        d['device_ts_iso'] = datetime.fromtimestamp(self.device_ts, tz=timezone.utc).isoformat()
        return d

class ProducerInterface:

    def start(self):
        pass

    def stop(self):
        pass

    def send_telemetry(self, record: Dict[str, Any]):
        raise NotImplementedError

    def send_alert(self, record: Dict[str, Any]):
        raise NotImplementedError

class FileProducer(ProducerInterface):
    def __init__(self, out_dir: str, telemetry_topic: str, alert_topic: str, flush_interval: float = 2.0):
        os.makedirs(out_dir, exist_ok=True)
        self.telemetry_path = os.path.join(out_dir, telemetry_topic.replace('.', '_') + '.ndjson')
        self.alert_path = os.path.join(out_dir, alert_topic.replace('.', '_') + '.ndjson')

        self._tf = open(self.telemetry_path, 'a', buffering=1)
        self._af = open(self.alert_path, 'a', buffering=1)
        self._t_lock = threading.Lock()
        self._a_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._flush_interval = flush_interval
        self._flusher = threading.Thread(target=self._flush_loop, daemon=True)

    def start(self):
        """Start background flusher thread."""
        self._flusher.start()

    def stop(self):
        """Stop background thread and close files."""
        self._stop_event.set()
        self._flusher.join(timeout=1)
        self._tf.close()
        self._af.close()

    def _flush_loop(self):
        """Periodically flush file buffers."""
        while not self._stop_event.wait(self._flush_interval):
            with self._t_lock, self._a_lock:
                self._tf.flush()
                self._af.flush()

    def send_telemetry(self, record: Dict[str, Any]):
        line = json.dumps(record, separators=(',', ':')) + '\n'
        with self._t_lock:
            self._tf.write(line)

    def send_alert(self, record: Dict[str, Any]):
        line = json.dumps(record, separators=(',', ':')) + '\n'
        with self._a_lock:
            self._af.write(line)

try:
    from aiokafka import AIOKafkaProducer
    import asyncio

    class KafkaProducer(ProducerInterface):
        def __init__(self, bootstrap_servers: str, telemetry_topic: str, alert_topic: str):
            self._bs = bootstrap_servers
            self._telemetry_topic = telemetry_topic
            self._alert_topic = alert_topic
            self._producer: Optional[AIOKafkaProducer] = None
            self._loop = asyncio.get_event_loop()

        async def start(self):
            self._producer = AIOKafkaProducer(bootstrap_servers=self._bs, loop=self._loop)
            await self._producer.start()

        async def stop(self):
            if self._producer:
                try:
                    await self._producer.stop()
                except Exception as e:
                    print("[KafkaProducer] Error while stopping:", e)

        def _ensure_running(self):
            if not self._producer:
                raise RuntimeError('KafkaProducer: call start() before sending')

        def send_telemetry(self, record: Dict[str, Any]):
            self._ensure_running()
            msg = json.dumps(record).encode()
            self._loop.create_task(self._safe_send(self._telemetry_topic, msg))

        def send_alert(self, record: Dict[str, Any]):
            self._ensure_running()
            msg = json.dumps(record).encode()
            self._loop.create_task(self._safe_send(self._alert_topic, msg))

        async def _safe_send(self, topic: str, msg: bytes):
            """Send with error handling."""
            try:
                await self._producer.send_and_wait(topic, msg)
            except Exception as e:
                print(f"[KafkaProducer] Failed to send to {topic}: {e}")

except Exception:
    KafkaProducer = None


def write_parquet_batch(records: List[Dict[str, Any]], out_path: str, debug_json: bool = False):
    if not records:
        return

    try:
        df = pd.DataFrame(records)
        table = pa.Table.from_pandas(df)
        pq.write_table(table, out_path)

        if debug_json:
            json_path = os.path.splitext(out_path)[0] + ".json"
            with open(json_path, "w") as f:
                json.dump(records, f, indent=2)

    except Exception as e:
        print(f"[write_parquet_batch] Failed to write {out_path}: {e}")

def get_list_idx(db_location: str, type_str: str) -> Dict[str, Optional[str]]:
    conn = sqlite3.connect(db_location)
    cur = conn.cursor()
    if type_str == 'device':
        cur.execute(f"SELECT device_id FROM farm_gateway_device")
        rows = cur.fetchall()
        result = {r[0]: None for r in rows}
    elif type_str == 'gateway':
        cur.execute(f"SELECT device_id, gateway_id FROM farm_gateway_device")
        rows = cur.fetchall()
        result = {r[0]: r[1] for r in rows}
    else:
        raise ValueError(f"Unknown type_str: {type_str}")
    conn.close()
    return result
