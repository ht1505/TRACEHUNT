#!/usr/bin/env python3
"""
TraceHunt - Spark Session Factory
Provides a unified SparkSession configured for HDFS access and standalone cluster execution.
Supports both distributed cluster execution and local fallback for testing.
"""

import os
from pathlib import Path
from pyspark.sql import SparkSession


def get_spark_session(app_name="TraceHunt-Analytics", master_url=None):
    """
    Initializes and returns a configured SparkSession.
    Checks environment variables for cluster Spark Master, falling back to local[*] if not set.
    """
    if master_url is None:
        master_url = os.environ.get("SPARK_MASTER", "local[*]")

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(master_url)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1g")
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def resolve_data_path(filename, hdfs_host=None):
    """
    Resolves dataset path either from HDFS or local filesystem.
    Priority:
    1. If HDFS_URL is set in environment or passed and the file is enabled
       through HDFS_INPUT_FILES (e.g. hdfs://master:9000/tracehunt/raw/)
    2. Fallback to local 'dataset/sample/' for local development/testing.
    """
    hdfs_base = os.environ.get("HDFS_BASE_URL", "")
    hdfs_files = {
        item.strip()
        for item in os.environ.get("HDFS_INPUT_FILES", "movement_events.csv").split(",")
        if item.strip()
    }
    if hdfs_host and filename in hdfs_files:
        return f"{hdfs_host}/tracehunt/raw/{filename}"
    elif hdfs_base and filename in hdfs_files:
        return f"{hdfs_base.rstrip('/')}/{filename}"

    # Check local paths for metadata files and development fallback.
    local_sample = Path("dataset") / "sample" / filename
    if local_sample.exists():
        return str(local_sample.resolve())
    local_output = Path("output") / "data" / filename
    if local_output.exists():
        return str(local_output.resolve())
    return filename
