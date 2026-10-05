#!/usr/bin/env python3
"""TRACEHUNT Spark analytics foundation.

This entry point keeps the initial Spark transformations independent from the
MongoDB/API pipeline. It is suitable for local[*] execution now and accepts
filesystem or HDFS paths for later deployment.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_INPUT = PROJECT_ROOT / "dataset" / "sample" / "movement_events.csv"
DEFAULT_LOCATIONS = PROJECT_ROOT / "dataset" / "sample" / "locations.csv"
DEFAULT_OUTPUT = PROJECT_ROOT / "output" / "spark_analytics"


def build_spark_session() -> SparkSession:
    """Create the local development Spark session required by this phase."""
    return (
        SparkSession.builder.appName("TRACEHUNT-PySpark-Analytics")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def load_clean_events(
    spark: SparkSession, input_path: str, locations_path: str
) -> DataFrame:
    """Read and normalize movement events, adding coordinates from locations."""
    raw_events = (
        spark.read.option("header", "true")
        .option("inferSchema", "false")
        .csv(input_path)
    )
    locations = (
        spark.read.option("header", "true")
        .option("inferSchema", "false")
        .csv(locations_path)
        .select(
            F.trim("location_id").alias("location_id"),
            F.col("latitude").cast("double").alias("latitude"),
            F.col("longitude").cast("double").alias("longitude"),
        )
    )

    normalized = (
        raw_events.select(
            F.trim("event_id").alias("event_id"),
            F.trim("person_id").alias("person_id"),
            F.to_timestamp(F.trim("timestamp"), "yyyy-MM-dd HH:mm:ss").alias(
                "timestamp"
            ),
            F.trim("location_id").alias("location_id"),
            F.trim("event_type").alias("event_type"),
            F.trim("transport_mode").alias("transport_mode"),
        )
        .join(locations, on="location_id", how="left")
        .filter(
            F.col("event_id").isNotNull()
            & F.col("person_id").isNotNull()
            & F.col("timestamp").isNotNull()
            & F.col("location_id").isNotNull()
            & F.col("event_type").isNotNull()
            & F.col("transport_mode").isNotNull()
            & F.col("latitude").between(-90.0, 90.0)
            & F.col("longitude").between(-180.0, 180.0)
        )
    )
    return normalized


def reconstruct_journeys(events: DataFrame) -> DataFrame:
    """Build one preliminary journey summary per person and calendar date."""
    return (
        events.withColumn("date", F.to_date("timestamp"))
        .groupBy("person_id", "date")
        .agg(
            F.min("timestamp").alias("start_time"),
            F.max("timestamp").alias("end_time"),
            F.count("*").alias("total_events"),
        )
        .select("person_id", "date", "start_time", "end_time", "total_events")
        .orderBy("person_id", "date")
    )


def detect_speed_anomalies(events: DataFrame, max_speed_kmh: float) -> DataFrame:
    """Flag implausible consecutive movements using Spark SQL expressions."""
    event_window = Window.partitionBy("person_id").orderBy(
        "timestamp", "event_id"
    )
    lagged = (
        events.withColumn("previous_timestamp", F.lag("timestamp").over(event_window))
        .withColumn("previous_location_id", F.lag("location_id").over(event_window))
        .withColumn("previous_latitude", F.lag("latitude").over(event_window))
        .withColumn("previous_longitude", F.lag("longitude").over(event_window))
    )
    elapsed_seconds = (
        F.col("timestamp").cast("long") - F.col("previous_timestamp").cast("long")
    )
    lat_delta = F.radians(F.col("latitude") - F.col("previous_latitude"))
    lon_delta = F.radians(F.col("longitude") - F.col("previous_longitude"))
    haversine_a = (
        F.pow(F.sin(lat_delta / 2), 2)
        + F.cos(F.radians("previous_latitude"))
        * F.cos(F.radians("latitude"))
        * F.pow(F.sin(lon_delta / 2), 2)
    )
    distance_km = 6371.0 * 2.0 * F.asin(F.sqrt(haversine_a))
    speed_kmh = distance_km / (elapsed_seconds / 3600.0)

    return (
        lagged.withColumn("elapsed_seconds", elapsed_seconds)
        .withColumn("distance_km", distance_km)
        .withColumn("speed_kmh", speed_kmh)
        .filter(
            F.col("previous_timestamp").isNotNull()
            & (F.col("location_id") != F.col("previous_location_id"))
            & F.col("elapsed_seconds").between(1, 7200)
            & (F.col("speed_kmh") > F.lit(max_speed_kmh))
        )
        .select(
            "person_id",
            "previous_location_id",
            "location_id",
            "previous_timestamp",
            "timestamp",
            "elapsed_seconds",
            F.round("distance_km", 3).alias("distance_km"),
            F.round("speed_kmh", 3).alias("speed_kmh"),
            "transport_mode",
        )
        .orderBy("person_id", "timestamp")
    )


def write_outputs(
    ordered_events: DataFrame,
    journeys: DataFrame,
    speed_anomalies: DataFrame,
    output_path: str,
) -> None:
    """Write Spark-managed CSV output directories for subsequent processing."""
    output_root = output_path.rstrip("/\\")
    (
        ordered_events.withColumn(
            "timestamp", F.date_format("timestamp", "yyyy-MM-dd HH:mm:ss")
        )
        .write.mode("overwrite")
        .option("header", "true")
        .csv(f"{output_root}/ordered_events")
    )
    (
        journeys.select(
            "person_id",
            "date",
            F.date_format("start_time", "yyyy-MM-dd HH:mm:ss").alias("start_time"),
            F.date_format("end_time", "yyyy-MM-dd HH:mm:ss").alias("end_time"),
            "total_events",
        )
        .write.mode("overwrite")
        .option("header", "true")
        .csv(f"{output_root}/journeys")
    )
    (
        speed_anomalies.withColumn(
            "previous_timestamp",
            F.date_format("previous_timestamp", "yyyy-MM-dd HH:mm:ss"),
        )
        .withColumn("timestamp", F.date_format("timestamp", "yyyy-MM-dd HH:mm:ss"))
        .write.mode("overwrite")
        .option("header", "true")
        .csv(f"{output_root}/speed_anomalies")
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default=str(DEFAULT_INPUT), help="Movement events CSV path")
    parser.add_argument(
        "--locations",
        default=str(DEFAULT_LOCATIONS),
        help="Locations CSV path used to enrich coordinates",
    )
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT),
        help="Output directory for Spark CSV result directories",
    )
    parser.add_argument(
        "--max-speed-kmh",
        type=float,
        default=130.0,
        help="Speed threshold for preliminary suspicious-movement analysis",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        print(f"Spark version: {spark.version}")
        print(f"Spark master: {spark.sparkContext.master}")
        print(f"Input path: {args.input}")
        print(f"Locations path: {args.locations}")

        events = load_clean_events(spark, args.input, args.locations).cache()
        records_loaded = (
            spark.read.option("header", "true").option("inferSchema", "false").csv(args.input).count()
        )
        records_after_cleaning = events.count()
        ordered_events = events.orderBy("person_id", "timestamp", "event_id")
        journeys = reconstruct_journeys(ordered_events).cache()
        speed_anomalies = detect_speed_anomalies(ordered_events, args.max_speed_kmh)

        people_processed = events.select("person_id").distinct().count()
        journeys_generated = journeys.count()
        suspicious_movements = speed_anomalies.count()
        write_outputs(ordered_events, journeys, speed_anomalies, args.output)

        print(f"Records loaded: {records_loaded}")
        print(f"Records after cleaning: {records_after_cleaning}")
        print(f"People processed: {people_processed}")
        print(f"Journeys generated: {journeys_generated}")
        print(f"Suspicious movements flagged: {suspicious_movements}")
        print(f"Ordered events output: {args.output.rstrip('/\\')}/ordered_events")
        print(f"Journey output: {args.output.rstrip('/\\')}/journeys")
        print(f"Speed analysis output: {args.output.rstrip('/\\')}/speed_anomalies")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
