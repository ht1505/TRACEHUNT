#!/usr/bin/env python3
"""
TraceHunt - MongoDB Schema & Index Initializer
Creates collections and establishes optimized indexes for investigation queries.
"""

import sys
import os
from pymongo import MongoClient, ASCENDING, DESCENDING

# Add workspace root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from mongodb.mongo_client import get_db, DB_NAME


def init_schema():
    """Initializes MongoDB database collections and compound indexes."""
    db = get_db()
    if db is None:
        print("[!] Warning: MongoDB is not currently running. Schema initialization deferred.")
        return False

    print(f"[*] Initializing MongoDB Database: '{DB_NAME}'...")

    # Collections to initialize
    collections = ["people", "locations", "journeys", "incidents", "anomalies", "route_correlations"]
    existing = db.list_collection_names()

    for col_name in collections:
        if col_name not in existing:
            db.create_collection(col_name)
            print(f"    [+] Created collection: {col_name}")

    # Build Indexes for Fast Viva & Dashboard Queries
    print("[*] Building performance indexes...")
    db.people.create_index([("person_id", ASCENDING)], unique=True)
    db.locations.create_index([("location_id", ASCENDING)], unique=True)
    db.journeys.create_index([("person_id", ASCENDING)])
    db.journeys.create_index([("date", ASCENDING)])
    db.incidents.create_index([("incident_id", ASCENDING)], unique=True)
    db.incidents.create_index([("timestamp", DESCENDING)])
    db.anomalies.create_index([("type", ASCENDING)])
    db.anomalies.create_index([("severity", ASCENDING)])
    db.anomalies.create_index([("person_id", ASCENDING)])
    db.anomalies.create_index([("location_id", ASCENDING)])

    print("[✔] MongoDB Schema & Indexes Initialized Successfully!")
    return True


if __name__ == "__main__":
    init_schema()
