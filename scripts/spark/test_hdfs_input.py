#!/usr/bin/env python3
"""Read the configured movement-events input and report its schema and row count."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from pyspark.sql import SparkSession

from src.processing.spark_session_factory import resolve_data_path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--hdfs-base-url",
        help="HDFS base URL, for example hdfs://localhost:9000/tracehunt/raw",
    )
    parser.add_argument(
        "--master",
        default=os.environ.get("SPARK_MASTER", "local[2]"),
        help="Spark master URL (default: local[2])",
    )
    args = parser.parse_args()

    if args.hdfs_base_url:
        os.environ["HDFS_BASE_URL"] = args.hdfs_base_url

    input_path = resolve_data_path("movement_events.csv")
    spark = (
        SparkSession.builder
        .appName("TraceHunt-HDFS-Input-Smoke-Test")
        .master(args.master)
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    try:
        events = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "false")
            .csv(input_path)
        )
        print(f"Input path: {input_path}")
        print(f"Schema: {events.schema.simpleString()}")
        print(f"Rows: {events.count()}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
