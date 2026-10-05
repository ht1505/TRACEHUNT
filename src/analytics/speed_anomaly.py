#!/usr/bin/env python3
"""
TraceHunt - Analytics Module 4: Impossible Movement & Speed Violation
Uses Spark window functions (lag) to calculate displacement, elapsed time, and transit velocity.
Flags unrealistic speeds (>130 km/h) as teleportation / impossible movement anomalies.
"""

import sys
import os
import math
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Add workspace root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from src.processing.spark_session_factory import get_spark_session, resolve_data_path
from mongodb.mongo_client import upsert_records


def haversine_km(lat1, lon1, lat2, lon2):
    """Calculates approximate distance in kilometers between two GPS coordinates."""
    if any(v is None for v in (lat1, lon1, lat2, lon2)):
        return 0.0
    r = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return r * c


def run_speed_anomaly_detection(max_allowed_speed_kmh=130.0):
    """
    Computes velocity between consecutive events per person using Spark Window lag.
    Flags instances where speed exceeds realistic transit threshold.
    """
    spark = get_spark_session(app_name="TraceHunt-SpeedAnomaly")

    events_path = resolve_data_path("movement_events.csv")
    locations_path = resolve_data_path("locations.csv")
    people_path = resolve_data_path("people.csv")

    print("[*] Running Spark Impossible Movement Analytics...")
    df_events = spark.read.option("header", "true").csv(events_path)
    df_locs = spark.read.option("header", "true").csv(locations_path)
    df_people = spark.read.option("header", "true").csv(people_path)

    # Join coordinates
    joined = df_events.join(df_locs, on="location_id", how="inner")

    # Define Window partitioned by person_id ordered by timestamp
    win = Window.partitionBy("person_id").orderBy("timestamp")

    # Use lag to obtain previous event's timestamp, location, and coordinates
    df_lagged = (
        joined
        .withColumn("prev_timestamp", F.lag("timestamp").over(win))
        .withColumn("prev_loc_id", F.lag("location_id").over(win))
        .withColumn("prev_loc_name", F.lag("location_name").over(win))
        .withColumn("prev_lat", F.lag("latitude").over(win))
        .withColumn("prev_lon", F.lag("longitude").over(win))
        .filter(F.col("prev_timestamp").isNotNull())
    )

    rows = df_lagged.select(
        "person_id", "timestamp", "location_id", "location_name", "latitude", "longitude",
        "prev_timestamp", "prev_loc_id", "prev_loc_name", "prev_lat", "prev_lon", "transport_mode"
    ).collect()

    anomalies = []
    anom_counter = 1

    for r in rows:
        # Same location means no transit displacement
        if r["location_id"] == r["prev_loc_id"]:
            continue

        t1 = datetime.strptime(r["prev_timestamp"], "%Y-%m-%d %H:%M:%S")
        t2 = datetime.strptime(r["timestamp"], "%Y-%m-%d %H:%M:%S")
        time_diff_sec = (t2 - t1).total_seconds()

        # Ignore multi-day gaps or negative time glitches
        if time_diff_sec <= 0 or time_diff_sec > 7200:
            continue

        lat1, lon1 = float(r["prev_lat"]), float(r["prev_lon"])
        lat2, lon2 = float(r["latitude"]), float(r["longitude"])

        dist_km = haversine_km(lat1, lon1, lat2, lon2)
        hours = time_diff_sec / 3600.0
        speed_kmh = dist_km / hours if hours > 0 else 0.0

        if speed_kmh > max_allowed_speed_kmh:
            anom_id = f"ANOM_SPEED_{anom_counter:03d}"
            anom_counter += 1
            doc = {
                "_id": anom_id,
                "anomaly_id": anom_id,
                "type": "IMPOSSIBLE_MOVEMENT",
                "severity": "CRITICAL",
                "person_id": r["person_id"],
                "origin_location": r["prev_loc_name"],
                "origin_time": r["prev_timestamp"],
                "destination_location": r["location_name"],
                "destination_time": r["timestamp"],
                "time_difference_minutes": round(time_diff_sec / 60.0, 1),
                "distance_km": round(dist_km, 2),
                "calculated_speed_kmh": round(speed_kmh, 1),
                "transport_mode": r["transport_mode"],
                "description": f"Physically impossible velocity ({round(speed_kmh, 1)} km/h) detected between {r['prev_loc_name']} and {r['location_name']}."
            }
            anomalies.append(doc)
            print(f"[!] Impossible speed flagged for {r['person_id']}: {round(dist_km, 1)} km in {round(time_diff_sec/60, 1)} min ({round(speed_kmh, 1)} km/h)")

    upsert_records("anomalies", anomalies, id_field="_id")
    spark.stop()
    return anomalies


if __name__ == "__main__":
    run_speed_anomaly_detection()
