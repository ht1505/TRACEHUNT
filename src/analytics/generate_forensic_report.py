#!/usr/bin/env python3
"""
TraceHunt - Executive Forensic Intelligence Report
Summarizes the Big Data findings into clean, readable intelligence briefing tables.
Answers: "What are we doing with the Big Data?"
"""

import sys
import os

# Add workspace root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from mongodb.mongo_client import get_db


def print_forensic_report():
    db = get_db()
    if db is None:
        print("[!] MongoDB offline. Please make sure MongoDB is started.")
        return

    print("\n" + "=" * 78)
    print("  TRACEHUNT: DISTRIBUTED BIG DATA FORENSIC INVESTIGATION REPORT")
    print("=" * 78)

    # 1. System Overview Metrics
    people_count = db.people.count_documents({})
    loc_count = db.locations.count_documents({})
    anom_count = db.anomalies.count_documents({})
    inc_count = db.incidents.count_documents({})
    journey_count = db.journeys.count_documents({})

    print("\n[📊 CLUSTER-WIDE DATA LAKE METRICS]")
    print(f"  • Monitored Population:  {people_count:,} Citizens")
    print(f"  • Tracked City Sectors:  {loc_count} Locations")
    print(f"  • Reconstructed Paths:   {journey_count} Full Trajectories")
    print(f"  • Active Incident Cases: {inc_count} Forensic Cases")
    print(f"  • Flagged Anomalies:     {anom_count} Behavioral Anomalies")

    # 2. Case Study 1: Incident INC_0001 Proximity
    inc = db.incidents.find_one({"incident_id": "INC_0001"})
    print("\n" + "-" * 78)
    print("  CASE 1: CRIME SCENE CO-PRESENCE INVESTIGATION")
    print("-" * 78)
    if inc:
        print(f"  Incident ID:   {inc.get('incident_id')} ({inc.get('incident_type')}, Severity: {inc.get('severity')})")
        print(f"  Crime Scene:   {inc.get('location_name')}")
        print(f"  Incident Time: {inc.get('timestamp')}")
        print(f"  Narrative:     {inc.get('description')}")
        print("\n  [🚨 CITIZENS DETECTED WITHIN 150m AND ±25 MINUTES]")
        print(f"  {'Person ID':<12} {'Full Name':<18} {'Occupation':<16} {'Distance':<10} {'Timestamp'}")
        print("  " + "-" * 72)
        for p in inc.get("nearby_people", [])[:6]:
            print(f"  {p.get('person_id'):<12} {p.get('full_name', 'Citizen'):<18} {p.get('occupation', 'N/A'):<16} {str(p.get('distance_meters')) + ' m':<10} {p.get('timestamp')}")
    else:
        print("  Incident INC_0001 has not been processed yet.")

    # 3. Case Study 2: Anomaly Intelligence (Speed & Crowd)
    print("\n" + "-" * 78)
    print("  CASE 2: DETECTED BEHAVIORAL & SPATIAL-TEMPORAL ANOMALIES")
    print("-" * 78)
    anomalies = list(db.anomalies.find({}).limit(5))
    if anomalies:
        for idx, a in enumerate(anomalies, 1):
            a_type = a.get("type")
            if a_type == "IMPOSSIBLE_MOVEMENT":
                print(f"  [{idx}] ⚡ IMPOSSIBLE VELOCITY (TELEPORTATION) DETECTED")
                print(f"      • Subject:      Person {a.get('person_id')}")
                print(f"      • Route:        {a.get('origin_location')} -> {a.get('destination_location')}")
                print(f"      • Travel Time:  {a.get('time_difference_minutes')} mins | Distance: {a.get('distance_km')} km")
                print(f"      • Speed:        {a.get('calculated_speed_kmh')} km/h  (Limit: 130 km/h)")
                print(f"      • Conclusion:   Device cloning or GPS spoofing suspected.")
            elif a_type == "CROWD_ACTIVITY_SPIKE":
                print(f"  [{idx}] 🚨 UNUSUAL CROWD SURGE ANOMALY DETECTED")
                print(f"      • Location:     {a.get('location_name')}")
                print(f"      • Time Window:  {a.get('time_window')}")
                print(f"      • Observed:     {a.get('observed_events_per_hour')} events/hour (Baseline Avg: {a.get('baseline_avg_events')})")
                print(f"      • Surge Ratio:  {a.get('surge_ratio')} above threshold")
                print(f"      • Conclusion:   Unplanned gathering / stampede hazard.")
            print()
    else:
        print("  No anomalies recorded yet.")

    # 4. Case Study 3: Person Journey Tracking
    j = db.journeys.find_one({"person_id": "P00023"})
    print("-" * 78)
    print("  CASE 3: SUSPECT JOURNEY TIMELINE RECONSTRUCTION (P00023)")
    print("-" * 78)
    if j:
        print(f"  Subject ID:     {j.get('person_id')}")
        print(f"  Total Mileage:  {j.get('total_distance_km')} km across {j.get('total_events')} recorded events")
        print(f"  Active Window:  {j.get('start_time')} to {j.get('end_time')}")
        print("\n  [FIRST 5 CHRONOLOGICAL MOVEMENTS]:")
        for step in j.get("timeline", [])[:5]:
            print(f"    • {step.get('timestamp')} -> {step.get('location_name')} ({step.get('event_type')} via {step.get('transport_mode')})")
    print("\n" + "=" * 78 + "\n")


if __name__ == "__main__":
    print_forensic_report()
