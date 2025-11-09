import asyncio
import random
import argparse
import ruamel.yaml as yaml
import time
from common import Telemetry, FileProducer, KafkaProducer
from gateway import Gateway
import math


def _make_device_id(farm_idx, gw_idx, dev_idx):
    return f"farm{farm_idx}-gw{gw_idx}-dev{dev_idx}"


async def device_task(farm_id, gateway_id, device_id, cfg, gateway_obj, duration):
    rate_per_min = cfg['simulation']['device_send_rate_per_min']
    interval = 60.0 / rate_per_min
    jitter_ms = cfg['simulation'].get('jitter_ms', 200)
    start = time.time()
    while time.time() - start < duration:
        # randomize interval to avoid sync
        sleep_time = max(0.001, random.gauss(interval, jitter_ms / 1000.0))
        await asyncio.sleep(sleep_time)
        ts = time.time()
        # basic synthetic values with some noise and diurnal pattern
        t = 20 + 10 * math.sin(ts / 3600.0) + random.uniform(-2, 2)
        h = 50 + 20 * math.cos(ts / 7200.0) + random.uniform(-5, 5)
        sm = 30 + random.uniform(-10, 10)
        light = max(0, 20000 * max(0, math.sin(ts / 86400.0)) + random.uniform(-2000, 2000))
        # small probability that device fails and reports extreme values
        if random.random() < 0.001:
            t += random.choice([-30, 30])
        # random device status
        dev_status = 'OK' if random.random() > 0.01 else 'OFFLINE'
        tel = Telemetry(
            farm_id=farm_id,
            gateway_id=gateway_id,
            device_id=device_id,
            ts=ts,
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
    for fi in range(sim['farms']):
        farm_id = f'farm{fi}'
        for gi in range(sim['gateways_per_farm']):
            gw_id = f'gw{fi}-{gi}'
            for di in range(sim['devices_per_gateway']):
                dev_id = _make_device_id(fi, gi, di)
                tasks.append(asyncio.create_task(device_task(farm_id, gw_id, dev_id, cfg, gateway_obj, duration)))
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
        prod = FileProducer(cfg['gateway'].get('out_dir', './data_simulator/out'),
                            cfg['gateway']['kafka_topic_telemetry'],
                            cfg['gateway']['kafka_topic_alerts'])
    else:
        prod = KafkaProducer(cfg['producer']['kafka_bootstrap_servers'],
                             cfg['gateway']['kafka_topic_telemetry'],
                             cfg['gateway']['kafka_topic_alerts'])

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
