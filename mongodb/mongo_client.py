#!/usr/bin/env python3
"""
TraceHunt - MongoDB Helper & Sink
Handles writing processed analytical results into MongoDB collections.
Includes graceful fallback caching to local JSON files if MongoDB is temporarily offline.
"""

import os
import json
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME = "tracehunt_db"


def get_db():
    """Returns a connected MongoDB database instance, or None if unavailable."""
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
        client.admin.command('ping')
        return client[DB_NAME]
    except (ConnectionFailure, ServerSelectionTimeoutError):
        return None


def upsert_records(collection_name, records, id_field="_id"):
    """
    Inserts or updates a list of dictionaries into the designated MongoDB collection.
    If MongoDB is offline, persists to 'output/mongo_backup/<collection_name>.json'.
    """
    db = get_db()
    if db is not None:
        col = db[collection_name]
        for rec in records:
            if id_field in rec:
                col.replace_one({id_field: rec[id_field]}, rec, upsert=True)
            else:
                col.insert_one(rec)
        print(f"[✔] Persisted {len(records)} records to MongoDB -> '{DB_NAME}.{collection_name}'")
    else:
        # Fallback to local JSON store
        backup_dir = os.path.join("output", "mongo_backup")
        os.makedirs(backup_dir, exist_ok=True)
        backup_file = os.path.join(backup_dir, f"{collection_name}.json")
        with open(backup_file, "w") as f:
            json.dump(records, f, indent=2, default=str)
        print(f"[!] MongoDB offline; saved {len(records)} records locally to: {backup_file}")
