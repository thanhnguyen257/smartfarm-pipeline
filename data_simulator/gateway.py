import threading
import time
import collections
import argparse
import ruamel.yaml as yaml
from common import Telemetry, FileProducer, KafkaProducer, write_parquet_batch, get_list_idx
import os
import inspect
import asyncio
import numpy as np
from datetime import datetime, timezone


def clamp(value, min_v, max_v):
    return max(min_v, min(value, max_v))

class Gateway:
    def __init__(self, cfg: dict, producer):
        self.cfg = cfg
        self.producer = producer
        self._q = collections.deque()
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._last_alert_ts = {}  # device_id -> last alert ts
        self._last_state = {}  # device_id -> last state
        self._last_info = collections.defaultdict(dict)  # device_id -> info dict
        self._aggregate_thread = threading.Thread(target=self._aggregate_loop, daemon=True)
        self._parquet_buffer = []
        self._map_device_gateway = get_list_idx(cfg['data']['locations_db'], 'gateway')
        os.makedirs(cfg['data'].get('out_dir', './data/out'), exist_ok=True)

    def start(self):
        if hasattr(self.producer, 'start') and callable(self.producer.start):
            if inspect.iscoroutinefunction(self.producer.start):
                try:
                    loop = asyncio.get_running_loop()
                except RuntimeError:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    loop.run_until_complete(self.producer.start())
                    loop.close()
                else:
                    loop.create_task(self.producer.start())
            else:
                self.producer.start()

        self._aggregate_thread.start()

    def stop(self):
        self._stop.set()
        self._aggregate_thread.join(timeout=2)

        if hasattr(self.producer, 'stop') and callable(self.producer.stop):
            if inspect.iscoroutinefunction(self.producer.stop):
                try:
                    loop = asyncio.get_running_loop()
                except RuntimeError:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    loop.run_until_complete(self.producer.stop())
                    loop.close()
                else:
                    loop.create_task(self.producer.stop())
            else:
                self.producer.stop()

    def ingest(self, telemetry: Telemetry):
        with self._lock:
            self._q.append(telemetry)

    def _compute_state(self, t: Telemetry):
        thr = self.cfg['alerts']['thresholds']
        level = 'OK'
        out = []
        for metric in ['temperature', 'humidity', 'soil_moisture', 'light_level']:
            v = getattr(t, metric)
            metric_thr = thr.get(metric, {})
            critical = metric_thr.get('critical')
            if critical is not None:
                if v < critical[0] or v > critical[1]:
                    out.append(metric)
                    level = 'CRITICAL'
        return level, out

    def _aggregate_loop(self):
        window = self.cfg['gateway']['aggregate_window_seconds']
        parquet_interval = self.cfg['gateway'].get('parquet_write_interval_seconds', 30)
        last_parquet = time.time()
        stop_event = self._stop
        while not stop_event.is_set():
            stop_event.wait(window)

            batch = []
            with self._lock:
                while self._q:
                    batch.append(self._q.popleft())

            if not batch:
                continue

            device_grouped = {}
            seen_devices = set()
            cooldown = self.cfg['alerts']['cooldown_seconds']
            for t in batch:
                t.temperature = clamp(t.temperature, -10, 60)       # temperature (°C)
                t.humidity = clamp(t.humidity, 0, 100)              # humidity (%)
                t.soil_moisture = clamp(t.soil_moisture, 0, 100)    # soil moisture (%)
                t.light_level = clamp(t.light_level, 0, 120000)     # lux

                device_grouped.setdefault(t.device_id, []).append(t)
                gateway_id = self._map_device_gateway[t.device_id]
                now = time.time()
                
                gateway_info = {
                    'gateway_id': gateway_id,
                    'gateway_ts': int(now * 1000),
                    'gateway_ts_iso': datetime.fromtimestamp(now, tz=timezone.utc).isoformat()
                }
                self._parquet_buffer.append(t.to_dict() | gateway_info)

                state, details = self._compute_state(t)
                prev = self._last_info.get(t.device_id, {})
                prev_state = prev.get('last_state', '')
                prev_alert = prev.get('last_alert_ts', 0)

                if (state == "OK" and prev_state == "CRITICAL") or \
                    (state == "CRITICAL" and (now - prev_alert) >= cooldown):
                    alert = {
                        **gateway_info,
                        'device_id': t.device_id,
                        'state': state,
                        'details': details,
                    }
                    
                    self.producer.send_alert(alert)
                    self._last_info[t.device_id]['last_alert_ts'] = now

                self._last_info[t.device_id]['last_state'] = state
                self._last_info[t.device_id]['last_ts'] = now
                seen_devices.add(t.device_id)

            offline_threshold = self.cfg['alerts']['offline_threshold_seconds']
            for device_id in self._map_device_gateway.keys() - seen_devices:
                prev = self._last_info.get(device_id, {})
                prev_seen = prev.get('last_ts', 0)
                now = time.time()
                if (now - prev_seen) >= offline_threshold:
                    alert = {
                        'gateway_id': self._map_device_gateway[device_id],
                        'device_id': device_id,
                        'gateway_ts': int(now * 1000),
                        'gateway_ts_iso': datetime.fromtimestamp(now, tz=timezone.utc).isoformat(),
                        'state': 'OFFLINE',
                        'details': [],
                    }
                    self.producer.send_alert(alert)
                    self._last_info[device_id]['last_alert_ts'] = now

            for device_id, items in device_grouped.items():
                data = np.array(
                    [(t.temperature, t.humidity, t.soil_moisture, t.light_level) for t in items],
                    dtype=[('temp', 'f4'), ('hum', 'f4'), ('soil', 'f4'), ('light', 'f4')]
                )
                now = time.time()
                out = {
                    'device_id': device_id,
                    'gateway_id': gateway_id,
                    'gateway_ts': int(now * 1000),
                    'gateway_ts_iso': datetime.fromtimestamp(now, tz=timezone.utc).isoformat(),
                    'count': len(items),
                    'temperature_avg': float(data['temp'].mean()),
                    'temperature_min': float(data['temp'].min()),
                    'temperature_max': float(data['temp'].max()),
                    'temperature_std': float(data['temp'].std()),
                    'humidity_avg': float(data['hum'].mean()),
                    'humidity_min': float(data['hum'].min()),
                    'humidity_max': float(data['hum'].max()),
                    'humidity_std': float(data['hum'].std()),
                    'soil_moisture_avg': float(data['soil'].mean()),
                    'soil_moisture_min': float(data['soil'].min()),
                    'soil_moisture_max': float(data['soil'].max()),
                    'soil_moisture_std': float(data['soil'].std()),
                    'light_level_avg': float(data['light'].mean()),
                    'light_level_min': float(data['light'].min()),
                    'light_level_max': float(data['light'].max()),
                    'light_level_std': float(data['light'].std()),
                }
                self.producer.send_telemetry(out)

            if time.time() - last_parquet >= parquet_interval and self._parquet_buffer:
                out_dir = self.cfg['data']['out_dir']
                fname = f"telemetry_{int(time.time())}.parquet"
                out_path = os.path.join(out_dir, fname)
                try:
                    write_parquet_batch(self._parquet_buffer, out_path)
                    self._parquet_buffer = []
                    last_parquet = time.time()
                except Exception as e:
                    print('Failed to write parquet:', e)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', default='config.yml')
    parser.add_argument('--mode', choices=['file', 'kafka'], default='file')
    args = parser.parse_args()
    yaml = yaml.YAML(typ='safe', pure=True)
    cfg = yaml.load(open(args.config))
    if args.mode == 'file':
        prod = FileProducer(cfg['data'].get('out_dir', './data/out'),
                            cfg['kafka']['topic_raw_telemetry'],
                            cfg['kafka']['topic_alerts'])
    else:
        prod = KafkaProducer(cfg['kafka']['bootstrap_servers'],
                             cfg['kafka']['topic_raw_telemetry'],
                             cfg['kafka']['topic_alerts'])
    gw = Gateway(cfg, prod)
    gw.start()
    print('Gateway started (press Ctrl-C to stop)')
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        gw.stop()
        print('Stopped')