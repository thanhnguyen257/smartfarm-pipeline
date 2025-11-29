# generate_locations.py
import sqlite3
import random
import math
import argparse
import os
import uuid
import ruamel.yaml as yaml

# Vietnam approx bounding box (lat, lon)
VN_LAT_MIN, VN_LAT_MAX = 8.0, 23.5
VN_LON_MIN, VN_LON_MAX = 102.0, 109.5

def random_point_in_radius(lat, lon, radius_m):
    # approximate: 1 deg lat ~ 111km, 1 deg lon ~ cos(lat)*111km
    r_km = radius_m / 1000.0
    # random distance and angle
    d = math.sqrt(random.random()) * r_km
    theta = random.random() * 2 * math.pi
    delta_lat = (d * math.cos(theta)) / 111.0
    delta_lon = (d * math.sin(theta)) / (111.0 * math.cos(math.radians(lat)))
    return lat + delta_lat, lon + delta_lon

def main(cfg):
    os.makedirs(os.path.dirname(cfg['sqlite']), exist_ok=True)
    conn = sqlite3.connect(cfg['sqlite'])
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS farm (id TEXT PRIMARY KEY, latitude REAL, longitude REAL)")
    cur.execute("CREATE TABLE IF NOT EXISTS gateway (id TEXT PRIMARY KEY, latitude REAL, longitude REAL)")
    cur.execute("CREATE TABLE IF NOT EXISTS device (id TEXT PRIMARY KEY, latitude REAL, longitude REAL)")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS farm_gateway_device (
        farm_id TEXT,
        gateway_id TEXT,
        device_id TEXT,
        PRIMARY KEY(farm_id, gateway_id, device_id),
        FOREIGN KEY(farm_id) REFERENCES farm(id),
        FOREIGN KEY(gateway_id) REFERENCES gateway(id),
        FOREIGN KEY(device_id) REFERENCES device(id)
    );
    """)

    cur.execute("DELETE FROM device")
    cur.execute("DELETE FROM gateway")
    cur.execute("DELETE FROM farm")
    cur.execute("DELETE FROM farm_gateway_device")

    farms = []
    for _ in range(cfg['simulation']['farms']):
        lat = random.uniform(VN_LAT_MIN, VN_LAT_MAX)
        lon = random.uniform(VN_LON_MIN, VN_LON_MAX)
        farm_id = str(uuid.uuid4())
        farms.append((farm_id, lat, lon))
        cur.execute("INSERT INTO farm (id, latitude, longitude) VALUES (?, ?, ?)", (farm_id, lat, lon))

    gateways = []
    for farm_id, flat, flon in farms:
        for _ in range(cfg['simulation']['gateways_per_farm']):
            # place gateway within a small radius around farm center (e.g., 2-5km)
            g_lat, g_lon = random_point_in_radius(flat, flon, random.uniform(500, 3000))
            gw_id = str(uuid.uuid4())
            gateways.append((farm_id, gw_id, g_lat, g_lon))
            cur.execute("INSERT INTO gateway (id, latitude, longitude) VALUES (?, ?, ?)", (gw_id, g_lat, g_lon))
    
    for farm_id, gw_id, g_lat, g_lon in gateways:
        for _ in range(cfg['simulation']['devices_per_gateway']):
            dev_id = str(uuid.uuid4())
            # device within radius of gateway (e.g., 50-500m)
            d_lat, d_lon = random_point_in_radius(g_lat, g_lon, random.uniform(20, 500))
            cur.execute("INSERT INTO device (id, latitude, longitude) VALUES (?, ?, ?)", (dev_id, d_lat, d_lon))
            cur.execute("INSERT INTO farm_gateway_device (farm_id, gateway_id, device_id) VALUES (?, ?, ?)", (farm_id, gw_id, dev_id))

    conn.commit()
    conn.close()
    print("Generated locations into", cfg['sqlite'])

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--sqlite", default="./data/locations.db")
    parser.add_argument('--config', default='config.yml')
    args = parser.parse_args()
    yaml = yaml.YAML(typ='safe', pure=True)
    cfg = yaml.load(open(args.config))
    cfg['sqlite'] = args.sqlite
    main(cfg)
