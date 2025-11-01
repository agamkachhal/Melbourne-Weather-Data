import os
import requests
import sqlite3
from dotenv import load_dotenv
import json

# Load environment variables
load_dotenv()

API_BASE_URL = os.getenv("API_BASE_URL")
PAGE_SIZE = int(os.getenv("PAGE_SIZE", 500))

## Pull data from the Weather data API and return required records

def fetch_data(min_records=1000):
    records = []
    offset = 0

    while len(records) < min_records:
        params = {"$limit": PAGE_SIZE, "$offset": offset}
        resp = requests.get(API_BASE_URL, params=params)
        data = resp.json()

        # extract actual records from "results"
        batch = data.get("results", [])
        if not isinstance(batch, list) or not batch:
            print("No more data returned from API.")
            break

        records.extend(batch)
        offset += len(batch)
        print(f"Fetched {len(records)} records so far...")

        if len(batch) < PAGE_SIZE:
            break

    print(f"Total records fetched: {len(records)}")
    return records[:min_records]


## Create table in SQLITE Database with the necessary columns

def create_table(conn):
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS sensor_readings")
    cur.execute(
        """
        CREATE TABLE sensor_readings (
            device_id TEXT,
            received_at TEXT,
            airtemperature FLOAT,
            relativehumidity FLOAT,
            sensorlocation TEXT
        )
        """
    )
    conn.commit()

# Insert records into the Database

def insert_records(conn, records):
    cur = conn.cursor()

    # Ensure each record is a dictionary before accessing keys
    valid_records = [r for r in records if isinstance(r, dict)]

    print(f"Inserting {len(valid_records)} valid records (skipping {len(records) - len(valid_records)} invalid ones)")

    for r in valid_records:
        cur.execute(
            """
            INSERT INTO sensor_readings(device_id, received_at, airtemperature, relativehumidity, sensorlocation)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                r.get("device_id"),
                r.get("received_at"),
                r.get("airtemperature"),
                r.get("relativehumidity"),
                str(r.get("sensorlocation")),
            ),
        )

    conn.commit()


def main():
    conn = sqlite3.connect("bloomeroo.db")
    create_table(conn)

    data = fetch_data(min_records=1000)
    print(f"Fetched {len(data)} records")

    insert_records(conn, data)
    print("Data inserted into SQLite database bloomeroo.db")

    conn.close()


if __name__ == "__main__":
    main()