#!/usr/bin/env python3
"""
TraceHunt - Analytics Module 3: Crowd Activity Spike Anomaly Detection
Identifies locations experiencing abnormal crowd density surges exceeding statistical baselines.
"""

import sys
import os
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Add workspace root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from src.processing.spark_session_factory import get_spark_session, resolve_data_path
from mongodb.mongo_client import upsert_records


def run_crowd_anomaly_detection():
    """
    Computes hourly activity per location using Spark aggregations.
    Detects locations with count > mean + 2.5 * stddev.
    """
    spark = get_spark_session(app_name="TraceHunt-CrowdAnomaly")

    events_path = resolve_data_path("movement_events.csv")
    locations_path = resolve_data_path("locations.csv")

    print("[*] Running Spark Crowd Surge Analytics...")
    df_events = spark.read.option("header", "true").csv(events_path)
    df_locs = spark.read.option("header", "true").csv(locations_path)

    # Extract date and hour from timestamp string (e.g. 2026-09-29 18:00:00)
    df_with_time = df_events.withColumn(
        "hour_bucket", F.substring(F.col("timestamp"), 1, 13)
    )

    # Hourly count per location
    hourly_counts = (
        df_with_time
        .groupBy("location_id", "hour_bucket")
        .agg(F.count("event_id").alias("hourly_events"))
    )

    # Compute baseline metrics (mean and stddev per location)
    loc_stats = (
        hourly_counts
        .groupBy("location_id")
        .agg(
            F.avg("hourly_events").alias("avg_hourly"),
            F.stddev("hourly_events").alias("std_hourly"),
            F.max("hourly_events").alias("max_hourly")
        )
        .fillna({"std_hourly": 1.0})
    )

    # Join back and flag surges
    enriched = (
        hourly_counts
        .join(loc_stats, on="location_id")
        .join(df_locs.select("location_id", "location_name", "capacity"), on="location_id")
    )

    # Condition: hourly_events > 300 AND hourly_events > (avg_hourly + 2.0 * std_hourly)
    spikes_df = (
        enriched
        .filter(
            (F.col("hourly_events") > 300) &
            (F.col("hourly_events") > (F.col("avg_hourly") + (F.col("std_hourly") * 2.0)))
        )
        .sort(F.desc("hourly_events"))
    )

    spike_rows = spikes_df.collect()
    print(f"[+] Flagged {len(spike_rows)} crowd surge time-windows.")

    anomalies = []
    for idx, row in enumerate(spike_rows, 1):
        anom_id = f"ANOM_CROWD_{idx:03d}"
        doc = {
            "_id": anom_id,
            "anomaly_id": anom_id,
            "type": "CROWD_ACTIVITY_SPIKE",
            "severity": "HIGH" if row["hourly_events"] > 1000 else "MEDIUM",
            "location_id": row["location_id"],
            "location_name": row["location_name"],
            "time_window": f"{row['hour_bucket']}:00:00",
            "observed_events_per_hour": int(row["hourly_events"]),
            "baseline_avg_events": round(float(row["avg_hourly"]), 1),
            "surge_ratio": f"{round(float(row['hourly_events']) / max(float(row['avg_hourly']), 1.0), 2)}x",
            "description": f"Unusual volume spike at {row['location_name']} with {row['hourly_events']} events logged in 1 hour."
        }
        anomalies.append(doc)

    upsert_records("anomalies", anomalies, id_field="_id")
    spark.stop()
    return anomalies


if __name__ == "__main__":
    run_crowd_anomaly_detection()
