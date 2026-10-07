"""TraceHunt Flask API and static dashboard server."""

import json
import logging
import os
import subprocess
import csv
from pathlib import Path

from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS

PROJECT_ROOT = Path(__file__).resolve().parents[1]
import sys

sys.path.append(str(PROJECT_ROOT))
from mongodb.mongo_client import get_db, mongo_is_available

logger = logging.getLogger(__name__)
app = Flask(
    __name__,
    static_folder=str(PROJECT_ROOT / "frontend"),
    static_url_path="",
)
CORS(app)


def load_fallback_json(collection_name):
    """Read the local analytics cache used when MongoDB is offline."""
    backup_path = PROJECT_ROOT / "output" / "mongo_backup" / f"{collection_name}.json"
    try:
        if not backup_path.exists():
            return []
        with backup_path.open(encoding="utf-8") as stream:
            data = json.load(stream)
        return data if isinstance(data, list) else []
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("Could not read fallback data %s: %s", backup_path, exc)
        return []


def load_fallback_people():
    """Read reference people only when the people collection/cache is absent."""
    people_path = PROJECT_ROOT / "dataset" / "sample" / "people.csv"
    try:
        with people_path.open(newline="", encoding="utf-8") as stream:
            return list(csv.DictReader(stream))
    except (OSError, csv.Error) as exc:
        logger.warning("Could not read fallback people data %s: %s", people_path, exc)
        return []


def _without_id(document):
    if document is not None:
        document.pop("_id", None)
    return document


def _records(collection_name, query=None):
    db = get_db()
    if db is not None:
        return [_without_id(item) for item in db[collection_name].find(query or {}, {"_id": 0})]
    records = load_fallback_json(collection_name)
    if collection_name == "people" and not records:
        records = load_fallback_people()
    if query:
        records = [
            record for record in records
            if all(record.get(key) == value for key, value in query.items())
        ]
    return records


def _record(collection_name, query):
    db = get_db()
    if db is not None:
        record = _without_id(db[collection_name].find_one(query, {"_id": 0}))
        if record is not None:
            return record
        if collection_name != "people":
            return None

    records = load_fallback_json(collection_name)
    if collection_name == "people" and not records:
        records = load_fallback_people()
    return next(
        (record for record in records
         if all(record.get(key) == value for key, value in query.items())),
        None,
    )


@app.errorhandler(400)
def bad_request(error):
    return jsonify({"error": "Invalid request", "message": str(error)}), 400


@app.errorhandler(404)
def not_found(error):
    return jsonify({"error": "Resource not found"}), 404


@app.errorhandler(500)
def server_error(error):
    logger.exception("Unhandled API error")
    return jsonify({"error": "Internal server error"}), 500


@app.route("/")
def index():
    return send_from_directory(str(PROJECT_ROOT / "frontend"), "index.html")


@app.route("/api/health", methods=["GET"])
def health():
    mongo_online = mongo_is_available()
    payload = {
        "status": "ok" if mongo_online else "degraded",
        "backend": "ok",
        "mongodb": "connected" if mongo_online else "unavailable",
    }
    return jsonify(payload), 200 if mongo_online else 503


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
            "database_status": "ONLINE (MongoDB Connected)",
        }
    else:
        stats = {
            "total_people": len(load_fallback_json("people")) or 1000,
            "total_locations": len(load_fallback_json("locations")) or 50,
            "total_incidents": len(load_fallback_json("incidents")) or 50,
            "total_anomalies": len(load_fallback_json("anomalies")),
            "total_journeys": len(load_fallback_json("journeys")),
            "database_status": "LOCAL FALLBACK CACHE",
        }
    return jsonify(stats)


def get_person(person_id):
    if not person_id.strip():
        return jsonify({"error": "person_id is required"}), 400
    person = _record("people", {"person_id": person_id})
    if person is None:
        return jsonify({"error": f"Person {person_id} not found"}), 404
    return jsonify(person)


app.add_url_rule("/api/person/<person_id>", "get_person", get_person, methods=["GET"])
app.add_url_rule("/api/people/<person_id>", "get_person_legacy", get_person, methods=["GET"])


@app.route("/api/journey/<person_id>", methods=["GET"])
def get_journey(person_id):
    if not person_id.strip():
        return jsonify({"error": "person_id is required"}), 400
    journey = _record("journeys", {"person_id": person_id})
    if journey is None:
        return jsonify({"error": f"No journey reconstructed yet for {person_id}"}), 404
    return jsonify(journey)


@app.route("/api/incidents", methods=["GET"])
def list_incidents():
    incidents = _records("incidents")
    if incidents:
        return jsonify(incidents)
    return jsonify([{
        "incident_id": "INC_0001",
        "location_name": "Grand Bazaar Sector-1",
        "timestamp": "2026-09-29 14:30:00",
        "incident_type": "Theft",
        "severity": "HIGH",
        "nearby_records_count": 4,
        "description": "Reported theft incident at Grand Bazaar.",
    }])


@app.route("/api/incidents/<incident_id>", methods=["GET"])
def get_incident(incident_id):
    incident = _record("incidents", {"incident_id": incident_id})
    if incident is None:
        return jsonify({"error": "Incident not found"}), 404
    return jsonify(incident)


@app.route("/api/anomalies", methods=["GET"])
def list_anomalies():
    query = {}
    for field in ("type", "severity", "person_id", "location_id"):
        value = request.args.get(field)
        if value:
            query[field] = value
    return jsonify(_records("anomalies", query))


@app.route("/api/anomalies/<person_id>", methods=["GET"])
def list_person_anomalies(person_id):
    if not person_id.strip():
        return jsonify({"error": "person_id is required"}), 400
    return jsonify(_records("anomalies", {"person_id": person_id}))


@app.route("/api/routes", methods=["GET"])
def list_routes():
    return jsonify(_records("route_correlations"))


@app.route("/api/cluster/status", methods=["GET"])
def cluster_status():
    hdfs_report = "Hadoop status not available"
    try:
        result = subprocess.run(
            ["hdfs", "dfsadmin", "-report"],
            capture_output=True,
            text=True,
            timeout=3,
            check=False,
        )
        if result.returncode == 0:
            hdfs_report = "\n".join(result.stdout.splitlines()[:10])
    except (OSError, subprocess.SubprocessError):
        logger.info("HDFS status command is unavailable")

    return jsonify({
        "cluster_name": "TraceHunt Distributed Cluster",
        "topology": {
            "master": "NameNode, ResourceManager, SparkMaster (192.168.x.x)",
            "worker1": "DataNode, NodeManager, SparkWorker (192.168.x.x)",
            "worker2": "DataNode, NodeManager, SparkWorker (192.168.x.x)",
        },
        "hdfs_summary": hdfs_report,
        "spark_master_url": os.environ.get("SPARK_MASTER", "spark://master:7077"),
    })


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5050))
    app.run(host="0.0.0.0", port=port, debug=False)
