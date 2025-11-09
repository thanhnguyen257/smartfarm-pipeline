import threading
import time
import collections
import argparse
import ruamel.yaml as yaml
from common import Telemetry, FileProducer, KafkaProducer, write_parquet_batch
import os


class Gateway:
    def __init__(self, cfg: dict, producer):
        self.cfg = cfg
        self.producer = producer
        self._q = collections.deque()
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._last_alert_ts = {}  # device_id -> last alert ts
        self._last_state = {}  # device_id -> last state
        self._aggregate_thread = threading.Thread(target=self._aggregate_loop, daemon=True)
        self._parquet_buffer = []
        os.makedirs(cfg['gateway'].get('out_dir', './data_simulator/out'), exist_ok=True)

    def start(self):
        if hasattr(self.producer, 'start') and callable(self.producer.start):
            import inspect, asyncio

            # Detect if the producer's start() is async
            if inspect.iscoroutinefunction(self.producer.start):
                try:
                    loop = asyncio.get_running_loop()
                except RuntimeError:
                    # No running loop, so create one temporarily
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    loop.run_until_complete(self.producer.start())
                    loop.close()
                else:
                    # Running inside an asyncio app (like simulator)
                    loop.create_task(self.producer.start())
            else:
                # Synchronous start method
                self.producer.start()

        self._aggregate_thread.start()

    def stop(self):
        self._stop.set()
        self._aggregate_thread.join(timeout=1)

    def ingest(self, telemetry: Telemetry):
        with self._lock:
            self._q.append(telemetry)

    def _agg_key(self, t: Telemetry):
        return (t.farm_id, t.gateway_id)

    def _compute_state(self, t: Telemetry):
        # simple state evaluation using thresholds
        thr = self.cfg['alerts']['thresholds']
        level = 'OK'
        out = []
        for metric in ['temperature', 'humidity', 'soil_moisture', 'light_level']:
            v = getattr(t, metric)
            metric_thr = thr.get(metric, {})
            warning = metric_thr.get('warning')
            critical = metric_thr.get('critical')
            state = 'OK'
            if warning is not None:
                if v < warning[0] or v > warning[1]:
                    state = 'WARNING'
            if critical is not None:
                if v < critical[0] or v > critical[1]:
                    state = 'CRITICAL'
            if state != 'OK':
                out.append((metric, state))
        if any(s == 'CRITICAL' for _, s in out):
            level = 'CRITICAL'
        elif any(s == 'WARNING' for _, s in out):
            level = 'WARNING'
        return level, out

    def _aggregate_loop(self):
        window = self.cfg['gateway']['aggregate_window_seconds']
        parquet_interval = self.cfg['gateway'].get('parquet_write_interval_seconds', 30)
        last_parquet = time.time()
        while not self._stop.is_set():
            time.sleep(window)
            batch = []
            with self._lock:
                while self._q:
                    batch.append(self._q.popleft())
            if not batch:
                continue
            # group by gateway
            grouped = {}
            for t in batch:
                k = self._agg_key(t)
                grouped.setdefault(k, []).append(t)
                # add to parquet buffer
                self._parquet_buffer.append(t.to_dict())
                # evaluate device state and send alerts on change with cooldown
                device_key = f"{t.gateway_id}:{t.device_id}"
                state, details = self._compute_state(t)
                prev = self._last_state.get(device_key)
                now = time.time()
                cooldown = self.cfg['alerts']['cooldown_seconds']
                last_alert = self._last_alert_ts.get(device_key, 0)
                if state != prev and (now - last_alert) >= cooldown:
                    # send alert depending on severity
                    alert = {
                        'farm_id': t.farm_id,
                        'gateway_id': t.gateway_id,
                        'device_id': t.device_id,
                        'ts': int(t.ts * 1000),
                        'new_state': state,
                        'details': details,
                    }
                    if state == 'CRITICAL':
                        self.producer.send_alert(alert)
                    else:
                        # still produce to telemetry topic as non-critical event if desired
                        self.producer.send_telemetry({'alert_info': alert})
                    self._last_state[device_key] = state
                    self._last_alert_ts[device_key] = now

            # produce aggregated summaries
            for k, items in grouped.items():
                farm_id, gateway_id = k
                temps = [i.temperature for i in items]
                hums = [i.humidity for i in items]
                out = {
                    'farm_id': farm_id,
                    'gateway_id': gateway_id,
                    'ts': int(time.time() * 1000),
                    'count': len(items),
                    'temperature_min': min(temps),
                    'temperature_max': max(temps),
                    'temperature_avg': sum(temps) / len(temps),
                    'humidity_avg': sum(hums) / len(hums),
                }
                self.producer.send_telemetry(out)

            # flush parquet buffer periodically
            if time.time() - last_parquet >= parquet_interval and self._parquet_buffer:
                out_dir = self.cfg['gateway']['out_dir']
                fname = f"telemetry_{int(time.time())}.parquet"
                out_path = os.path.join(out_dir, fname)
                try:
                    write_parquet_batch(self._parquet_buffer, out_path)
                    self._parquet_buffer = []
                    last_parquet = time.time()
                except Exception as e:
                    print('Failed to write parquet', e)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', default='config.yml')
    parser.add_argument('--mode', choices=['file', 'kafka'], default='file')
    args = parser.parse_args()
    yaml = yaml.YAML(typ='safe', pure=True)
    cfg = yaml.load(open(args.config))
    if args.mode == 'file':
        prod = FileProducer(cfg['gateway'].get('out_dir', './data_simulator/out'), cfg['gateway']['kafka_topic_telemetry'], cfg['gateway']['kafka_topic_alerts'])
    else:
        # for kafka mode, user is responsible for starting the async loop and producer
        prod = KafkaProducer(cfg['producer']['kafka_bootstrap_servers'], cfg['gateway']['kafka_topic_telemetry'], cfg['gateway']['kafka_topic_alerts'])
    gw = Gateway(cfg, prod)
    gw.start()
    print('Gateway started (press Ctrl-C to stop)')
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        gw.stop()
        print('Stopped')