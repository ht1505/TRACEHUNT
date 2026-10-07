#!/usr/bin/env python3
"""Shared MongoDB connection and processed-result persistence helpers."""

import json
import logging
import os
from pathlib import Path
from typing import Iterable, Mapping

from pymongo import MongoClient
from pymongo.errors import PyMongoError

PROJECT_ROOT = Path(__file__).resolve().parents[1]
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://127.0.0.1:27017")
DB_NAME = "tracehunt"
MONGO_TIMEOUT_MS = int(os.environ.get("MONGO_TIMEOUT_MS", "1500"))
logger = logging.getLogger(__name__)

_client = None


def get_db():
    """Return the configured database, or ``None`` when MongoDB is unavailable."""
    global _client
    try:
        if _client is None:
            _client = MongoClient(
                MONGO_URI,
                serverSelectionTimeoutMS=MONGO_TIMEOUT_MS,
                connectTimeoutMS=MONGO_TIMEOUT_MS,
            )
        _client.admin.command("ping")
        return _client[DB_NAME]
    except (PyMongoError, ValueError) as exc:
        logger.warning("MongoDB unavailable: %s", exc)
        return None


def mongo_is_available() -> bool:
    """Return whether the configured MongoDB server responds to a ping."""
    return get_db() is not None


def upsert_records(collection_name: str, records: Iterable[Mapping], id_field="_id"):
    """
    Inserts or updates a list of dictionaries into the designated MongoDB collection.
    If MongoDB is offline, persists to 'output/mongo_backup/<collection_name>.json'.
    """
    records = [dict(record) for record in records]
    db = get_db()
    if db is not None:
        col = db[collection_name]
        for rec in records:
            if id_field in rec:
                col.replace_one({id_field: rec[id_field]}, rec, upsert=True)
            else:
                col.insert_one(rec)
        print(f"[+] Persisted {len(records)} records to MongoDB -> '{DB_NAME}.{collection_name}'")
    else:
        backup_dir = PROJECT_ROOT / "output" / "mongo_backup"
        backup_dir.mkdir(parents=True, exist_ok=True)
        backup_file = backup_dir / f"{collection_name}.json"
        with backup_file.open("w", encoding="utf-8") as f:
            json.dump(records, f, indent=2, default=str)
        print(f"[!] MongoDB offline; saved {len(records)} records locally to: {backup_file}")
