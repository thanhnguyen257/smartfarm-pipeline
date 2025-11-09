
from dataclasses import dataclass, asdict
from typing import Dict, Any
import threading
import os
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


@dataclass
class Telemetry:
    farm_id: str
    gateway_id: str
    device_id: str
    ts: float
    temperature: float
    humidity: float
    soil_moisture: float
    light_level: float
    device_status: str

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d['ts'] = int(self.ts * 1000)
        return d


class ProducerInterface:
    def send_telemetry(self, record: Dict[str, Any]):
        raise NotImplementedError

    def send_alert(self, record: Dict[str, Any]):
        raise NotImplementedError


class FileProducer(ProducerInterface):
    def __init__(self, out_dir: str, telemetry_topic: str, alert_topic: str):
        os.makedirs(out_dir, exist_ok=True)
        self.telemetry_file = os.path.join(out_dir, telemetry_topic.replace('.', '_') + '.ndjson')
        self.alert_file = os.path.join(out_dir, alert_topic.replace('.', '_') + '.ndjson')
        self._t_lock = threading.Lock()
        self._a_lock = threading.Lock()

    def send_telemetry(self, record: Dict[str, Any]):
        with self._t_lock:
            with open(self.telemetry_file, 'a') as f:
                f.write(json.dumps(record) + '\n')

    def send_alert(self, record: Dict[str, Any]):
        with self._a_lock:
            with open(self.alert_file, 'a') as f:
                f.write(json.dumps(record) + '\n')


try:
    from aiokafka import AIOKafkaProducer
    import asyncio

    class KafkaProducer(ProducerInterface):
        def __init__(self, bootstrap_servers: str, telemetry_topic: str, alert_topic: str):
            self._bs = bootstrap_servers
            self._telemetry_topic = telemetry_topic
            self._alert_topic = alert_topic
            self._producer = None
            self._loop = asyncio.get_event_loop()

        async def start(self):
            self._producer = AIOKafkaProducer(bootstrap_servers=self._bs, loop=self._loop)
            await self._producer.start()

        async def stop(self):
            if self._producer:
                await self._producer.stop()

        def _ensure_running(self):
            if not self._producer:
                raise RuntimeError('Start the producer with start() first')

        def send_telemetry(self, record: Dict[str, Any]):
            self._ensure_running()
            self._loop.create_task(self._producer.send_and_wait(self._telemetry_topic, json.dumps(record).encode()))

        def send_alert(self, record: Dict[str, Any]):
            self._ensure_running()
            self._loop.create_task(self._producer.send_and_wait(self._alert_topic, json.dumps(record).encode()))

except Exception:
    KafkaProducer = None


# small parquet helper (batch write)
def write_parquet_batch(records, out_path: str):
    if not records:
        return
    df = pd.DataFrame(records)
    table = pa.Table.from_pandas(df)
    pq.write_table(table, out_path)
    json_path = os.path.splitext(out_path)[0] + ".json"
    with open(json_path, "w") as f:
        json.dump(records, f, indent=2)