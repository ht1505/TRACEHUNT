#!/usr/bin/env python3
"""
TraceHunt - Analytics Module 2: Incident Proximity Search
Finds individuals who were present near an incident location within a specific time window.
Calculates spatial distance and updates MongoDB with investigation case files.
"""

import sys
import os
import math
from datetime import datetime, timedelta
from pyspark.sql import functions as F

# Add workspace root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from src.processing.spark_session_factory import get_spark_session, resolve_data_path
from mongodb.mongo_client import upsert_records


def calculate_distance_meters(lat1, lon1, lat2, lon2):
    """Calculates geodesic distance in meters."""
    if any(v is None for v in (lat1, lon1, lat2, lon2)):
        return 999999.0
    r = 6371000.0  # Earth radius in meters
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return r * c


def run_incident_investigation(target_incident_id=None, window_minutes=25, max_radius_meters=800):
    """
    Executes distributed incident proximity search.
    Correlates incident time/location with movement events and people profiles.
    """
    spark = get_spark_session(app_name="TraceHunt-IncidentInvestigation")

    incidents_path = resolve_data_path("incidents.csv")
    events_path = resolve_data_path("movement_events.csv")
    locations_path = resolve_data_path("locations.csv")
    people_path = resolve_data_path("people.csv")

    print(f"[*] Loading datasets for Incident Proximity Analysis...")
    df_incidents = spark.read.option("header", "true").csv(incidents_path)
    df_events = spark.read.option("header", "true").csv(events_path)
    df_locations = spark.read.option("header", "true").csv(locations_path)
    df_people = spark.read.option("header", "true").csv(people_path)

    if target_incident_id:
        target_incidents = df_incidents.filter(F.col("incident_id") == target_incident_id).collect()
    else:
        # Default: investigate the top 10 incidents including INC_0001
        target_incidents = df_incidents.limit(10).collect()

    # Pre-join events with coordinates and people info
    events_enriched = (
        df_events
        .join(df_locations, on="location_id", how="inner")
        .join(df_people, on="person_id", how="left")
        .select(
            "event_id", "person_id", "full_name", "occupation", "device_type",
            "timestamp", "location_id", "location_name", "latitude", "longitude", "event_type"
        )
    )

    incident_results = []
    for inc in target_incidents:
        inc_id = inc["incident_id"]
        inc_time = datetime.strptime(inc["timestamp"], "%Y-%m-%d %H:%M:%S")
        inc_loc_id = inc["location_id"]
        inc_lat = float(inc.asDict().get("latitude", 0.0) or 0.0)
        inc_lon = float(inc.asDict().get("longitude", 0.0) or 0.0)

        # Lookup location coords if not in incident table
        if inc_lat == 0.0 or inc_lon == 0.0:
            loc_match = df_locations.filter(F.col("location_id") == inc_loc_id).collect()
            if loc_match:
                inc_lat = float(loc_match[0]["latitude"])
                inc_lon = float(loc_match[0]["longitude"])

        t_start = (inc_time - timedelta(minutes=window_minutes)).strftime("%Y-%m-%d %H:%M:%S")
        t_end = (inc_time + timedelta(minutes=window_minutes)).strftime("%Y-%m-%d %H:%M:%S")

        # Spark Filter: Temporal window
        temporal_candidates = events_enriched.filter(
            (F.col("timestamp") >= t_start) & (F.col("timestamp") <= t_end)
        ).collect()

        nearby_people = []
        seen_persons = set()

        for cand in temporal_candidates:
            pid = cand["person_id"]
            if pid in seen_persons:
                continue

            c_lat = float(cand["latitude"]) if cand["latitude"] else 0.0
            c_lon = float(cand["longitude"]) if cand["longitude"] else 0.0

            # If same location_id or within max_radius_meters
            dist = calculate_distance_meters(inc_lat, inc_lon, c_lat, c_lon)
            if cand["location_id"] == inc_loc_id or dist <= max_radius_meters:
                seen_persons.add(pid)
                nearby_people.append({
                    "person_id": pid,
                    "full_name": cand["full_name"],
                    "occupation": cand["occupation"],
                    "timestamp": cand["timestamp"],
                    "location_name": cand["location_name"],
                    "distance_meters": round(dist, 1),
                    "event_type": cand["event_type"]
                })

        # Sort by distance
        nearby_people.sort(key=lambda x: x["distance_meters"])

        doc = {
            "_id": inc_id,
            "incident_id": inc_id,
            "timestamp": inc["timestamp"],
            "location_id": inc_loc_id,
            "location_name": inc["location_name"],
            "incident_type": inc["incident_type"],
            "severity": inc["severity"],
            "description": inc["description"],
            "search_window_minutes": window_minutes,
            "nearby_records_count": len(nearby_people),
            "nearby_people": nearby_people[:20],  # Top 20 for clean dashboard view
            "status": "INVESTIGATED"
        }
        incident_results.append(doc)
        print(f"[+] Incident {inc_id} at {inc['location_name']}: Found {len(nearby_people)} nearby people.")

    upsert_records("incidents", incident_results, id_field="_id")
    spark.stop()
    return incident_results


if __name__ == "__main__":
    inc_arg = sys.argv[1] if len(sys.argv) > 1 else None
    run_incident_investigation(inc_arg)
