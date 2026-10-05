#!/usr/bin/env python3
"""
TraceHunt - Synthetic Dataset Generator with Seeded Patterns
Generates realistic city movement, transaction, and incident logs.
Includes intentionally planted forensic patterns for Spark analytics demonstration.
"""

import os
import sys
import csv
import json
import random
import argparse
from datetime import datetime, timedelta

# Configurable scale profiles
SCALE_PROFILES = {
    "small": {
        "num_people": 1000,
        "num_locations": 50,
        "num_events": 100000,
        "num_transactions": 25000,
        "num_incidents": 50,
        "days": 1
    },
    "medium": {
        "num_people": 10000,
        "num_locations": 200,
        "num_events": 1000000,
        "num_transactions": 250000,
        "num_incidents": 200,
        "days": 3
    },
    "large": {
        "num_people": 50000,
        "num_locations": 500,
        "num_events": 10000000,
        "num_transactions": 2000000,
        "num_incidents": 500,
        "days": 7
    }
}

FIRST_NAMES = [
    "Aarav", "Aditi", "Amit", "Ananya", "Arjun", "Deepak", "Divya", "Gaurav",
    "Ishaan", "Kavya", "Manish", "Meera", "Neha", "Nikhil", "Pooja", "Pranav",
    "Priya", "Rahul", "Riya", "Rohan", "Sanjay", "Shreya", "Siddharth", "Sneha",
    "Tanvi", "Varun", "Vikram", "Yash", "Aisha", "Karan", "Simran", "Kabir"
]

LAST_NAMES = [
    "Sharma", "Verma", "Patel", "Mehta", "Iyer", "Rao", "Reddy", "Nair",
    "Kapoor", "Malhotra", "Singh", "Gupta", "Joshi", "Bhat", "Deshmukh", "Kulkarni",
    "Chopra", "Das", "Banerjee", "Chatterjee", "Saxena", "Mishra", "Pandey", "Trivedi"
]

OCCUPATIONS = [
    "Software Engineer", "Student", "Retail Worker", "Banker", "Doctor",
    "Teacher", "Accountant", "Sales Manager", "Architect", "Civil Engineer",
    "Graphic Designer", "Research Analyst", "Consultant", "Delivery Partner"
]

AGE_GROUPS = ["18-25", "26-35", "36-50", "50+"]
AREAS = ["AREA_NORTH", "AREA_SOUTH", "AREA_EAST", "AREA_WEST", "AREA_CENTRAL"]
LOCATION_TYPES = ["Transport", "Commercial", "Residential", "Entertainment", "Educational", "Healthcare"]
EVENT_TYPES = ["CHECK_IN", "TAP_IN", "TAP_OUT", "TRANSIT_PASS", "CHECK_OUT"]
TRANSPORT_MODES = ["Metro", "Bus", "Walking", "Cab", "Personal_Vehicle"]
MERCHANT_TYPES = ["Cafe", "Restaurant", "Transit", "Supermarket", "Pharmacy", "Electronics"]
PAYMENT_METHODS = ["UPI", "Credit_Card", "Debit_Card", "Transit_Card", "Cash"]
INCIDENT_TYPES = ["Theft", "Vandalism", "Crowd_Surge", "Lost_Asset", "Unauthorized_Entry"]
SEVERITY_LEVELS = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]

# Base coordinates for simulated city (Center: Surat / Ahmedabad region)
CITY_BASE_LAT = 21.1702
CITY_BASE_LON = 72.8311


def generate_people(count):
    """Generates master records for citizens/people."""
    people = []
    for i in range(1, count + 1):
        person_id = f"P{i:05d}"
        name = f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"
        age = random.choice(AGE_GROUPS)
        occ = random.choice(OCCUPATIONS)
        home = random.choice(AREAS)
        device = random.choice(["Android", "iOS"])
        people.append({
            "person_id": person_id,
            "full_name": name,
            "age_group": age,
            "occupation": occ,
            "home_area": home,
            "device_type": device
        })
    return people


def generate_locations(count):
    """Generates city locations with geographic coordinates."""
    locations = []
    names = [
        "Central Station", "Tech Park North", "City Mall", "University Campus",
        "Grand Bazaar", "Civic Hospital", "Aerodrome Terminal", "Riverside Walk",
        "Cyber City", "National Library", "Sports Complex", "Financial Towers",
        "Old Quarter Market", "Botanical Gardens", "Harbor Port", "Suburban Plaza"
    ]
    for i in range(1, count + 1):
        loc_id = f"L{i:03d}"
        base_name = names[(i - 1) % len(names)]
        loc_name = f"{base_name} Sector-{((i - 1) // len(names)) + 1}"
        loc_type = random.choice(LOCATION_TYPES)
        area = random.choice(AREAS)
        # Distribute coordinates within ~25 km radius
        lat_offset = (random.random() - 0.5) * 0.22
        lon_offset = (random.random() - 0.5) * 0.22
        lat = round(CITY_BASE_LAT + lat_offset, 6)
        lon = round(CITY_BASE_LON + lon_offset, 6)
        capacity = random.randint(500, 15000)

        locations.append({
            "location_id": loc_id,
            "location_name": loc_name,
            "location_type": loc_type,
            "area": area,
            "latitude": lat,
            "longitude": lon,
            "capacity": capacity
        })
    return locations


def generate_incidents(count, locations, base_date):
    """Generates reference incidents occurring at specific locations and times."""
    incidents = []
    for i in range(1, count + 1):
        inc_id = f"INC_{i:04d}"
        loc = random.choice(locations)
        hours = random.randint(8, 22)
        minutes = random.randint(0, 59)
        seconds = random.randint(0, 59)
        inc_time = base_date + timedelta(hours=hours, minutes=minutes, seconds=seconds)
        inc_type = random.choice(INCIDENT_TYPES)
        severity = random.choice(SEVERITY_LEVELS)
        desc = f"Reported {inc_type} incident at {loc['location_name']}."

        incidents.append({
            "incident_id": inc_id,
            "timestamp": inc_time.strftime("%Y-%m-%d %H:%M:%S"),
            "location_id": loc["location_id"],
            "location_name": loc["location_name"],
            "incident_type": inc_type,
            "severity": severity,
            "description": desc
        })
    return incidents


def generate_datasets(profile_name="small", output_dir="output/data"):
    """Master generator function creating all CSV streams with seeded anomalies."""
    os.makedirs(output_dir, exist_ok=True)
    cfg = SCALE_PROFILES.get(profile_name, SCALE_PROFILES["small"])
    base_date = datetime(2026, 9, 29, 0, 0, 0)

    print(f"[*] Generating '{profile_name.upper()}' dataset:")
    print(f"    - People: {cfg['num_people']:,}")
    print(f"    - Locations: {cfg['num_locations']:,}")
    print(f"    - Movement Events: {cfg['num_events']:,}")
    print(f"    - Transactions: {cfg['num_transactions']:,}")
    print(f"    - Incidents: {cfg['num_incidents']:,}")

    # 1. Master People
    people = generate_people(cfg["num_people"])
    people_file = os.path.join(output_dir, "people.csv")
    with open(people_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=people[0].keys())
        writer.writeheader()
        writer.writerows(people)
    print(f"[+] Written: {people_file}")

    # 2. Master Locations
    locations = generate_locations(cfg["num_locations"])
    locations_file = os.path.join(output_dir, "locations.csv")
    with open(locations_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=locations[0].keys())
        writer.writeheader()
        writer.writerows(locations)
    print(f"[+] Written: {locations_file}")

    # 3. Master Incidents
    incidents = generate_incidents(cfg["num_incidents"], locations, base_date)
    # Ensure INC_0001 is fixed at L005 for clean demonstration
    if incidents:
        incidents[0]["incident_id"] = "INC_0001"
        incidents[0]["location_id"] = locations[4]["location_id"]
        incidents[0]["location_name"] = locations[4]["location_name"]
        incidents[0]["timestamp"] = "2026-09-29 14:30:00"
        incidents[0]["incident_type"] = "Theft"
        incidents[0]["severity"] = "HIGH"
        incidents[0]["description"] = f"Reported Theft incident at {locations[4]['location_name']}."

    incidents_file = os.path.join(output_dir, "incidents.csv")
    with open(incidents_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=incidents[0].keys())
        writer.writeheader()
        writer.writerows(incidents)
    print(f"[+] Written: {incidents_file}")

    # ----------------------------------------------------
    # Seeded Forensic Patterns Setup
    # ----------------------------------------------------
    planted_summary = {
        "incident_investigation": {
            "incident_id": "INC_0001",
            "location_id": locations[4]["location_id"],
            "location_name": locations[4]["location_name"],
            "timestamp": "2026-09-29 14:30:00",
            "planted_nearby_persons": ["P00023", "P00045", "P00078", "P00102"]
        },
        "impossible_speed_anomalies": [
            {
                "person_id": "P00023",
                "origin_location": locations[0]["location_id"],
                "origin_time": "2026-09-29 10:00:00",
                "dest_location": locations[-1]["location_id"],
                "dest_time": "2026-09-29 10:04:30",
                "description": "Travelled across city (~28 km) in 4.5 minutes (Speed > 370 km/h)"
            }
        ],
        "crowd_spike_anomalies": [
            {
                "location_id": locations[2]["location_id"],
                "location_name": locations[2]["location_name"],
                "window": "2026-09-29 18:00:00 to 19:00:00",
                "planted_extra_visitors": int(cfg["num_events"] * 0.04),
                "description": "Sudden anomalous surge 5x above baseline capacity"
            }
        ],
        "correlated_route_persons": ["P00311", "P00312"]
    }

    # 4. Movement Events (Stream to file in chunks to maintain low memory usage)
    events_file = os.path.join(output_dir, "movement_events.csv")
    fieldnames = ["event_id", "person_id", "timestamp", "location_id", "event_type", "transport_mode", "device_id"]

    print(f"[*] Generating {cfg['num_events']:,} movement events in streaming batches...")
    total_events_written = 0
    batch_size = 50000

    with open(events_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        # Seed Specific Pattern A: Planted Incident Proximity Events
        inc_loc = planted_summary["incident_investigation"]["location_id"]
        for p_id in planted_summary["incident_investigation"]["planted_nearby_persons"]:
            # Events around 14:20 to 14:35 at the incident location
            offset_mins = random.randint(-10, 8)
            e_time = datetime(2026, 9, 29, 14, 30, 0) + timedelta(minutes=offset_mins, seconds=random.randint(0, 50))
            writer.writerow({
                "event_id": f"EVT_SEEDED_{total_events_written + 1:07d}",
                "person_id": p_id,
                "timestamp": e_time.strftime("%Y-%m-%d %H:%M:%S"),
                "location_id": inc_loc,
                "event_type": "CHECK_IN",
                "transport_mode": "Walking",
                "device_id": f"DEV_{p_id}"
            })
            total_events_written += 1

        # Seed Specific Pattern B: Impossible Speed Events for P00023
        writer.writerow({
            "event_id": f"EVT_SEEDED_{total_events_written + 1:07d}",
            "person_id": "P00023",
            "timestamp": "2026-09-29 10:00:00",
            "location_id": locations[0]["location_id"],
            "event_type": "TAP_IN",
            "transport_mode": "Metro",
            "device_id": "DEV_P00023"
        })
        total_events_written += 1
        writer.writerow({
            "event_id": f"EVT_SEEDED_{total_events_written + 1:07d}",
            "person_id": "P00023",
            "timestamp": "2026-09-29 10:04:30",
            "location_id": locations[-1]["location_id"],
            "event_type": "TAP_OUT",
            "transport_mode": "Metro",
            "device_id": "DEV_P00023"
        })
        total_events_written += 1

        # Seed Specific Pattern C: Crowd Spike at locations[2] between 18:00 and 19:00
        spike_loc = locations[2]["location_id"]
        num_spike = planted_summary["crowd_spike_anomalies"][0]["planted_extra_visitors"]
        for _ in range(num_spike):
            sec_offset = random.randint(0, 3599)
            spike_time = datetime(2026, 9, 29, 18, 0, 0) + timedelta(seconds=sec_offset)
            rand_person = f"P{random.randint(1, cfg['num_people']):05d}"
            writer.writerow({
                "event_id": f"EVT_SPIKE_{total_events_written + 1:07d}",
                "person_id": rand_person,
                "timestamp": spike_time.strftime("%Y-%m-%d %H:%M:%S"),
                "location_id": spike_loc,
                "event_type": "CHECK_IN",
                "transport_mode": "Metro",
                "device_id": f"DEV_{rand_person}"
            })
            total_events_written += 1

        # Generate Remaining Background Movement Events
        batch = []
        loc_ids = [loc["location_id"] for loc in locations]
        people_ids = [p["person_id"] for p in people]

        remaining = cfg["num_events"] - total_events_written
        for i in range(remaining):
            p_id = random.choice(people_ids)
            loc_id = random.choice(loc_ids)
            # Random time across working hours (07:00 to 22:00)
            rand_sec = random.randint(7 * 3600, 22 * 3600)
            day_offset = random.randint(0, cfg["days"] - 1)
            e_time = base_date + timedelta(days=day_offset, seconds=rand_sec)

            batch.append({
                "event_id": f"EVT_{total_events_written + 1:08d}",
                "person_id": p_id,
                "timestamp": e_time.strftime("%Y-%m-%d %H:%M:%S"),
                "location_id": loc_id,
                "event_type": random.choice(EVENT_TYPES),
                "transport_mode": random.choice(TRANSPORT_MODES),
                "device_id": f"DEV_{p_id}"
            })
            total_events_written += 1

            if len(batch) >= batch_size:
                writer.writerows(batch)
                batch = []
                print(f"    ... {total_events_written:,} / {cfg['num_events']:,} events written")

        if batch:
            writer.writerows(batch)

    print(f"[+] Written: {events_file} ({total_events_written:,} records)")

    # 5. Financial Transactions Stream
    tx_file = os.path.join(output_dir, "transactions.csv")
    tx_fields = ["transaction_id", "person_id", "timestamp", "location_id", "amount", "merchant_type", "payment_method"]
    print(f"[*] Generating {cfg['num_transactions']:,} transactions...")

    with open(tx_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=tx_fields)
        writer.writeheader()
        tx_batch = []
        for i in range(1, cfg["num_transactions"] + 1):
            p_id = random.choice(people_ids)
            loc_id = random.choice(loc_ids)
            rand_sec = random.randint(8 * 3600, 21 * 3600)
            day_offset = random.randint(0, cfg["days"] - 1)
            t_time = base_date + timedelta(days=day_offset, seconds=rand_sec)
            amount = round(random.uniform(20.0, 4500.0), 2)

            tx_batch.append({
                "transaction_id": f"TXN_{i:07d}",
                "person_id": p_id,
                "timestamp": t_time.strftime("%Y-%m-%d %H:%M:%S"),
                "location_id": loc_id,
                "amount": amount,
                "merchant_type": random.choice(MERCHANT_TYPES),
                "payment_method": random.choice(PAYMENT_METHODS)
            })
            if len(tx_batch) >= batch_size:
                writer.writerows(tx_batch)
                tx_batch = []

        if tx_batch:
            writer.writerows(tx_batch)

    print(f"[+] Written: {tx_file}")

    # Write Planted Pattern Summary for Viva Demonstration
    summary_file = os.path.join(output_dir, "planted_patterns_summary.json")
    with open(summary_file, "w") as f:
        json.dump(planted_summary, f, indent=2)
    print(f"[+] Planted Patterns Summary saved to: {summary_file}")
    print("\n[✔] Dataset Generation Complete!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="TraceHunt Synthetic Dataset Generator")
    parser.add_argument("--scale", choices=["small", "medium", "large"], default="small",
                        help="Scale profile: small (100k), medium (1M), large (10M)")
    parser.add_argument("--out", default="dataset/sample",
                        help="Output directory path (default: dataset/sample)")
    args = parser.parse_args()

    generate_datasets(profile_name=args.scale, output_dir=args.out)
