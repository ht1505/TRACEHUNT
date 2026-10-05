#!/usr/bin/env python3
"""
TraceHunt - Flask REST API Backend
Serves investigation endpoints to the Web Dashboard.
Interacts with MongoDB for low-latency operational data retrieval
and provides cluster health status.
"""

import os
import sys
import json
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS

# Add workspace root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from mongodb.mongo_client import get_db

app = Flask(__name__, static_folder="../frontend", static_url_path="")
CORS(app)


def load_fallback_json(collection_name):
    """Fallback reader if MongoDB is offline during local dev."""
    backup_path = os.path.join("output", "mongo_backup", f"{collection_name}.json")
    if os.path.exists(backup_path):
        with open(backup_path, "r") as f:
            return json.load(f)
    return []


# ==============================================================================
# Static Dashboard Route
# ==============================================================================
@app.route("/")
def index():
    return send_from_directory("../frontend", "index.html")


# ==============================================================================
# Overview & System Statistics
# ==============================================================================
@app.route("/api/overview", methods=["GET"])
def get_overview():
    db = get_db()
    if db is not None:
        stats = {
            "total_people": db.people.count_documents({}),
            "total_locations": db.locations.count_documents({}),
            "total_incidents": db.incidents.count_documents({}),
            "total_anomalies": db.anomalies.count_documents({}),
            "total_journeys": db.journeys.count_documents({}),
            "database_status": "ONLINE (MongoDB Connected)"
        }
    else:
        anoms = load_fallback_json("anomalies")
        incs = load_fallback_json("incidents")
        journeys = load_fallback_json("journeys")
        stats = {
            "total_people": 1000,
            "total_locations": 50,
            "total_incidents": len(incs) if incs else 50,
            "total_anomalies": len(anoms) if anoms else 4,
            "total_journeys": len(journeys) if journeys else 6,
            "database_status": "LOCAL FALLBACK CACHE"
        }
    return jsonify(stats)


# ==============================================================================
# Person & Journey Investigation
# ==============================================================================
@app.route("/api/people/<person_id>", methods=["GET"])
def get_person(person_id):
    db = get_db()
    if db is not None:
        person = db.people.find_one({"person_id": person_id}, {"_id": 0})
        if person:
            return jsonify(person)
    return jsonify({"person_id": person_id, "name": f"Citizen {person_id}", "status": "Recorded"})


@app.route("/api/journey/<person_id>", methods=["GET"])
def get_journey(person_id):
    db = get_db()
    if db is not None:
        journey = db.journeys.find_one({"person_id": person_id}, {"_id": 0})
        if journey:
            return jsonify(journey)

    # Check fallback cache
    journeys = load_fallback_json("journeys")
    for j in journeys:
        if j.get("person_id") == person_id:
            return jsonify(j)

    return jsonify({"error": f"No journey reconstructed yet for {person_id}"}), 404


# ==============================================================================
# Incident Investigation
# ==============================================================================
@app.route("/api/incidents", methods=["GET"])
def list_incidents():
    db = get_db()
    if db is not None:
        incidents = list(db.incidents.find({}, {"_id": 0}))
        if incidents:
            return jsonify(incidents)

    # Check fallback cache
    fallback = load_fallback_json("incidents")
    if fallback:
        return jsonify(fallback)

    # If no analyzed cases yet, return sample reference
    return jsonify([
        {
            "incident_id": "INC_0001",
            "location_name": "Grand Bazaar Sector-1",
            "timestamp": "2026-09-29 14:30:00",
            "incident_type": "Theft",
            "severity": "HIGH",
            "nearby_records_count": 4,
            "description": "Reported theft incident at Grand Bazaar."
        }
    ])


@app.route("/api/incidents/<incident_id>", methods=["GET"])
def get_incident(incident_id):
    db = get_db()
    if db is not None:
        inc = db.incidents.find_one({"incident_id": incident_id}, {"_id": 0})
        if inc:
            return jsonify(inc)

    fallback = load_fallback_json("incidents")
    for inc in fallback:
        if inc.get("incident_id") == incident_id:
            return jsonify(inc)

    return jsonify({"error": "Incident not found"}), 404


# ==============================================================================
# Anomaly Intelligence
# ==============================================================================
@app.route("/api/anomalies", methods=["GET"])
def list_anomalies():
    anomaly_type = request.args.get("type")
    query = {"type": anomaly_type} if anomaly_type else {}

    db = get_db()
    if db is not None:
        anomalies = list(db.anomalies.find(query, {"_id": 0}))
        if anomalies:
            return jsonify(anomalies)

    fallback = load_fallback_json("anomalies")
    if anomaly_type:
        fallback = [a for a in fallback if a.get("type") == anomaly_type]
    return jsonify(fallback)


# ==============================================================================
# Route Similarity
# ==============================================================================
@app.route("/api/routes", methods=["GET"])
def list_routes():
    db = get_db()
    if db is not None:
        routes = list(db.route_correlations.find({}, {"_id": 0}))
        if routes:
            return jsonify(routes)
    return jsonify(load_fallback_json("route_correlations"))


# ==============================================================================
# Cluster Health Status (For Viva Demonstration)
# ==============================================================================
@app.route("/api/cluster/status", methods=["GET"])
def cluster_status():
    import subprocess
    hdfs_report = "Hadoop status not available"
    try:
        res = subprocess.run(["hdfs", "dfsadmin", "-report"], capture_output=True, text=True, timeout=3)
        if res.returncode == 0:
            lines = res.stdout.split("\n")[:10]
            hdfs_report = "\n".join(lines)
    except Exception:
        pass

    return jsonify({
        "cluster_name": "TraceHunt Distributed Cluster",
        "topology": {
            "master": "NameNode, ResourceManager, SparkMaster (192.168.x.x)",
            "worker1": "DataNode, NodeManager, SparkWorker (192.168.x.x)",
            "worker2": "DataNode, NodeManager, SparkWorker (192.168.x.x)"
        },
        "hdfs_summary": hdfs_report,
        "spark_master_url": os.environ.get("SPARK_MASTER", "spark://master:7077")
    })


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5050))
    print(f"[*] Starting TraceHunt Flask API on port {port}...")
    app.run(host="0.0.0.0", port=port, debug=True)
