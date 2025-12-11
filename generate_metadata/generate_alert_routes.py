import sqlite3
import random
import argparse

def generate_random_email(farm_id):
    suffix = random.randint(1, 999)
    return f"farm{farm_id}_{suffix}@example.com"

def main(args):
    conn = sqlite3.connect(args.loc_db)
    cur = conn.cursor()
    cur.execute("SELECT id FROM farm")
    farm_ids = [row[0] for row in cur.fetchall()]
    conn.close()

    print("Found farms:", farm_ids)

    conn = sqlite3.connect(args.route_db)
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS alert_routes")
    cur.execute("""
        CREATE TABLE alert_routes (
            farm_id TEXT,
            email TEXT
        )
    """)

    for farm_id in farm_ids:
        num_emails = random.randint(1, 3)
        for _ in range(num_emails):
            email = generate_random_email(farm_id)
            cur.execute("INSERT INTO alert_routes (farm_id, email) VALUES (?, ?)", 
                        (farm_id, email))
            print(f"Added route: {farm_id} -> {email}")

    conn.commit()
    conn.close()

    print("\nCreated alert_routes.db successfully.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--loc_db", default="./data/locations.db")
    parser.add_argument("--route_db", default="./data/alert_routes.db")
    args = parser.parse_args()
    main(args)
