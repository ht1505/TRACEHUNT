"""Load processed local analytics results into MongoDB without duplicating records."""

import argparse
import json
from pathlib import Path
from typing import Dict, List

from mongodb.mongo_client import PROJECT_ROOT, get_db, upsert_records

COLLECTION_KEYS = {
    "journeys": "_id",
    "anomalies": "_id",
}


def _read_json(path: Path) -> List[dict]:
    if not path.exists():
        return []
    with path.open(encoding="utf-8") as stream:
        data = json.load(stream)
    if not isinstance(data, list):
        raise ValueError(f"Expected a JSON list in {path}")
    return [item for item in data if isinstance(item, dict)]


def load_processed_results(output_dir: str = "output/mongo_backup") -> Dict[str, int]:
    """Upsert locally generated journeys and anomalies into the configured database.

    The existing analytics pipeline writes these JSON files when MongoDB is offline,
    so this loader also provides a retry path once MongoDB becomes available.
    """
    source_dir = Path(output_dir)
    if not source_dir.is_absolute():
        source_dir = PROJECT_ROOT / source_dir

    if get_db() is None:
        raise ConnectionError("MongoDB is unavailable; processed results were not loaded")

    counts = {}
    for collection, id_field in COLLECTION_KEYS.items():
        records = _read_json(source_dir / f"{collection}.json")
        upsert_records(collection, records, id_field=id_field)
        counts[collection] = len(records)
    return counts


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", default="output/mongo_backup")
    args = parser.parse_args()
    print(load_processed_results(args.output_dir))


if __name__ == "__main__":
    main()
