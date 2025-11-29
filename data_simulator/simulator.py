import asyncio
import random
import argparse
import ruamel.yaml as yaml
import time
from common import Telemetry, FileProducer, KafkaProducer, get_list_idx
from gateway import Gateway
import math


async def device_task(device_id, cfg, gateway_obj, duration):
    rate_per_min = cfg['simulation']['device_send_rate_per_min']
    interval = 60.0 / rate_per_min
    jitter_ms = cfg['simulation'].get('jitter_ms', 200)
    offline_prob = cfg['simulation'].get('offline_prob', 0.01)
    offline_min, offline_max = cfg['simulation'].get('offline_seconds', [10,60])

    start = time.time()
    offline_until = 0

    while time.time() - start < duration:
        now = time.time()
        if now < offline_until:
            await asyncio.sleep(interval)
            continue

        if random.random() < offline_prob:
            offline_until = now + random.uniform(offline_min, offline_max)
            continue

        sleep_time = max(0.001, random.gauss(interval, jitter_ms / 1000.0))
        await asyncio.sleep(sleep_time)

        ts = time.time()
        t = 20 + 10 * math.sin(ts / 3600.0) + random.uniform(-2, 2)
        h = 50 + 20 * math.cos(ts / 7200.0) + random.uniform(-5, 5)
        sm = 30 + random.uniform(-10, 10)
        light = max(0, 20000 * max(0, math.sin(ts / 86400.0)) + random.uniform(-2000, 2000))

        if random.random() < 0.001:
            t += random.choice([-30, 30])

        dev_status = 'OK'
        tel = Telemetry(
            device_id=device_id,
            device_ts=ts,
            temperature=round(t, 2),
            humidity=round(h, 2),
            soil_moisture=round(sm, 2),
            light_level=round(light, 2),
            device_status=dev_status,
        )
        gateway_obj.ingest(tel)


async def run_sim(cfg, gateway_obj):
    sim = cfg['simulation']
    tasks = []
    duration = sim['duration_seconds']
    device_ids = get_list_idx(cfg['data']['locations_db'], 'device').keys()
    for dev_id in device_ids:
        tasks.append(asyncio.create_task(device_task(dev_id, cfg, gateway_obj, duration)))
    print(f'Launched {len(tasks)} device tasks')
    await asyncio.gather(*tasks)

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', default='./data_simulator/config.yml')
    parser.add_argument('--mode', choices=['file', 'kafka'], default='file')
    args = parser.parse_args()

    yaml_loader = yaml.YAML(typ='safe', pure=True)
    cfg = yaml_loader.load(open(args.config))

    if args.mode == 'file':
        prod = FileProducer(cfg['data'].get('out_dir', './data_simulator/out'),
                            cfg['kafka']['topic_raw_telemetry'],
                            cfg['kafka']['topic_alerts'])
    else:
        prod = KafkaProducer(cfg['kafka']['bootstrap_servers'],
                             cfg['kafka']['topic_raw_telemetry'],
                             cfg['kafka']['topic_alerts'])

    gw = Gateway(cfg, prod)
    gw.start()

    print(f"Simulation started in {args.mode.upper()} mode")
    await run_sim(cfg, gw)
    print('Simulation finished')

    gw.stop()
    if args.mode == 'kafka':
        await prod.stop()


if __name__ == '__main__':
    asyncio.run(main())
