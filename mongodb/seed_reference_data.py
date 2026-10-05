#!/usr/bin/env python3
"""
TraceHunt - Reference Data Seeder
Loads static master entities (People and Locations) from CSV into MongoDB.
Allows instant UI lookup of entity profiles during investigations.
"""

import sys
import os
import csv

# Add workspace root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from mongodb.mongo_client import upsert_records


def seed_reference_data(data_dir="dataset/sample"):
    """Reads people.csv and locations.csv and seeds MongoDB collections."""
    people_file = os.path.join(data_dir, "people.csv")
    locations_file = os.path.join(data_dir, "locations.csv")

    if not os.path.exists(people_file) or not os.path.exists(locations_file):
        print(f"[!] Reference CSVs not found in {data_dir}. Run dataset generator first.")
        return

    # 1. Seed People
    print("[*] Seeding People master records into MongoDB...")
    people = []
    with open(people_file, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            row["_id"] = row["person_id"]
            people.append(row)
    upsert_records("people", people, id_field="_id")

    # 2. Seed Locations
    print("[*] Seeding Locations master records into MongoDB...")
    locations = []
    with open(locations_file, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            row["_id"] = row["location_id"]
            row["latitude"] = float(row["latitude"])
            row["longitude"] = float(row["longitude"])
            row["capacity"] = int(row["capacity"])
            locations.append(row)
    upsert_records("locations", locations, id_field="_id")

    print("[✔] Master Reference Data Seeding Complete!")


if __name__ == "__main__":
    target_dir = sys.argv[1] if len(sys.argv) > 1 else "dataset/sample"
    seed_reference_data(target_dir)
