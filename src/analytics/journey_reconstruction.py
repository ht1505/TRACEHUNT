#!/usr/bin/env python3
"""
TraceHunt - Analytics Module 1: Chronological Journey Reconstruction
Reconstructs person journeys in chronological order, calculates total movement distance,
and stores structured journey histories into MongoDB.
"""

import sys
import os
import math
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
    r = 6371.0  # Earth radius in km
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return r * c


def run_journey_reconstruction(target_person_id=None):
    """
    Executes distributed journey reconstruction across movement events.
    If target_person_id is specified, focuses on that individual; otherwise summarizes active individuals.
    """
    spark = get_spark_session(app_name="TraceHunt-JourneyReconstruction")

    events_path = resolve_data_path("movement_events.csv")
    locations_path = resolve_data_path("locations.csv")
    people_path = resolve_data_path("people.csv")

    print(f"[*] Reading datasets for Journey Reconstruction...")
    df_events = spark.read.option("header", "true").csv(events_path)
    df_locs = spark.read.option("header", "true").csv(locations_path)
    df_people = spark.read.option("header", "true").csv(people_path)

    # Join events with location coordinates
    df_joined = df_events.join(df_locs, on="location_id", how="inner")

    # If specific person requested, filter first
    if target_person_id:
        df_target = df_joined.filter(F.col("person_id") == target_person_id)
    else:
        # For batch demonstration, pick top 100 people or specific seeded candidates
        seeded_ids = ["P00023", "P00045", "P00078", "P00102", "P00311", "P00312"]
        df_target = df_joined.filter(F.col("person_id").isin(seeded_ids))

    # Window partitioned by person_id ordered chronologically
    win = Window.partitionBy("person_id").orderBy("timestamp")

    # Collect ordered rows
    rows = (
        df_target
        .select("person_id", "timestamp", "location_id", "location_name", "latitude", "longitude", "event_type", "transport_mode")
        .sort("person_id", "timestamp")
        .collect()
    )

    # Group into structured journey documents
    journeys_by_person = {}
    for r in rows:
        pid = r["person_id"]
        if pid not in journeys_by_person:
            journeys_by_person[pid] = {
                "_id": f"JOURNEY_{pid}_20260929",
                "person_id": pid,
                "date": "2026-09-29",
                "timeline": [],
                "total_events": 0,
                "total_distance_km": 0.0
            }

        lat = float(r["latitude"]) if r["latitude"] else 0.0
        lon = float(r["longitude"]) if r["longitude"] else 0.0

        timeline = journeys_by_person[pid]["timeline"]
        if timeline:
            prev = timeline[-1]
            dist = haversine_km(prev["latitude"], prev["longitude"], lat, lon)
            journeys_by_person[pid]["total_distance_km"] += dist

        timeline.append({
            "timestamp": r["timestamp"],
            "location_id": r["location_id"],
            "location_name": r["location_name"],
            "latitude": lat,
            "longitude": lon,
            "event_type": r["event_type"],
            "transport_mode": r["transport_mode"]
        })
        journeys_by_person[pid]["total_events"] += 1

    # Round distances and finalize
    records = []
    for pid, j in journeys_by_person.items():
        j["total_distance_km"] = round(j["total_distance_km"], 2)
        j["start_time"] = j["timeline"][0]["timestamp"] if j["timeline"] else None
        j["end_time"] = j["timeline"][-1]["timestamp"] if j["timeline"] else None
        records.append(j)

    print(f"[✔] Reconstructed {len(records)} detailed journeys.")
    upsert_records("journeys", records, id_field="_id")
    spark.stop()
    return records


if __name__ == "__main__":
    target = sys.argv[1] if len(sys.argv) > 1 else None
    run_journey_reconstruction(target)
