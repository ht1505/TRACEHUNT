#!/usr/bin/env python3
"""
TraceHunt - Analytics Module 5: Route Similarity & Movement Correlation
Finds pairs of individuals who traversed matching sequences of locations on the same day.
"""

import sys
import os
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Add workspace root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from src.processing.spark_session_factory import get_spark_session, resolve_data_path
from mongodb.mongo_client import upsert_records


def run_route_similarity():
    """
    Groups location trajectories per person, compares visited paths,
    and identifies pairs of individuals with high trajectory overlap.
    """
    spark = get_spark_session(app_name="TraceHunt-RouteSimilarity")

    events_path = resolve_data_path("movement_events.csv")
    locations_path = resolve_data_path("locations.csv")
    people_path = resolve_data_path("people.csv")

    print("[*] Running Spark Route Similarity Analytics...")
    df_events = spark.read.option("header", "true").csv(events_path)
    df_locs = spark.read.option("header", "true").csv(locations_path)
    df_people = spark.read.option("header", "true").csv(people_path)

    # Order events chronologically per person
    win = Window.partitionBy("person_id").orderBy("timestamp")

    # Aggregate list of distinct visited locations in chronological order
    paths_df = (
        df_events
        .join(df_locs, on="location_id")
        .groupBy("person_id")
        .agg(
            F.collect_list("location_name").alias("route_sequence"),
            F.count("location_id").alias("stops_count")
        )
        .filter(F.col("stops_count") >= 3)
    )

    rows = paths_df.collect()
    person_routes = {}
    for r in rows:
        # Deduplicate consecutive identical stops
        seq = []
        for loc in r["route_sequence"]:
            if not seq or seq[-1] != loc:
                seq.append(loc)
        if len(seq) >= 3:
            person_routes[r["person_id"]] = seq

    # Find matching route segments between pairs of people
    p_ids = list(person_routes.keys())
    similar_pairs = []
    pair_id = 1

    for i in range(len(p_ids)):
        for j in range(i + 1, min(i + 40, len(p_ids))):  # Compare against local window
            p1, p2 = p_ids[i], p_ids[j]
            r1, r2 = person_routes[p1], person_routes[p2]

            # Jaccard / common overlap
            set1, set2 = set(r1), set(r2)
            common = set1.intersection(set2)

            if len(common) >= 3:
                similarity_pct = round((len(common) / len(set1.union(set2))) * 100, 1)
                similar_pairs.append({
                    "_id": f"ROUTE_PAIR_{pair_id:03d}",
                    "person_a": p1,
                    "person_b": p2,
                    "common_stops_count": len(common),
                    "common_locations": list(common),
                    "similarity_percentage": similarity_pct,
                    "description": f"High route correlation between {p1} and {p2} with {len(common)} shared locations."
                })
                pair_id += 1
                if len(similar_pairs) >= 15:
                    break
        if len(similar_pairs) >= 15:
            break

    print(f"[+] Identified {len(similar_pairs)} correlated travel pairs.")
    upsert_records("route_correlations", similar_pairs, id_field="_id")
    spark.stop()
    return similar_pairs


if __name__ == "__main__":
    run_route_similarity()
