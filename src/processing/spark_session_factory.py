#!/usr/bin/env python3
"""
TraceHunt - Spark Session Factory
Provides a unified SparkSession configured for HDFS access and standalone cluster execution.
Supports both distributed cluster execution and local fallback for testing.
"""

import os
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
    1. If HDFS_URL is set in environment or passed (e.g. hdfs://master:9000/tracehunt/raw/)
    2. Fallback to local 'dataset/sample/' for local development/testing.
    """
    hdfs_base = os.environ.get("HDFS_BASE_URL", "")
    if hdfs_host:
        return f"{hdfs_host}/tracehunt/raw/{filename}"
    elif hdfs_base:
        return f"{hdfs_base.rstrip('/')}/{filename}"
    else:
        # Check local paths
        local_sample = os.path.join("dataset", "sample", filename)
        if os.path.exists(local_sample):
            return f"file://{os.path.abspath(local_sample)}"
        local_output = os.path.join("output", "data", filename)
        if os.path.exists(local_output):
            return f"file://{os.path.abspath(local_output)}"
        return filename
