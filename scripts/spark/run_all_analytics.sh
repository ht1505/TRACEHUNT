#!/usr/bin/env bash
# ==============================================================================
# TraceHunt - Master Spark Analytics Batch Runner
# Executes all 5 PySpark analytical modules and persists results to MongoDB.
# Supports both standalone cluster (spark://master:7077) and local development.
# ==============================================================================

set -e

SPARK_TARGET="${1:-${SPARK_MASTER:-local[*]}}"
echo "=========================================================="
echo " TraceHunt: Executing Spark Distributed Analytics Pipeline"
echo " Master Target: ${SPARK_TARGET}"
echo "=========================================================="

export SPARK_MASTER="${SPARK_TARGET}"

echo ""
echo "[1/5] Running Chronological Journey Reconstruction..."
python3 src/analytics/journey_reconstruction.py

echo ""
echo "[2/5] Running Spatial-Temporal Incident Investigation..."
python3 src/analytics/incident_investigation.py

echo ""
echo "[3/5] Running Crowd Density Surge Anomaly Detection..."
python3 src/analytics/crowd_anomaly.py

echo ""
echo "[4/5] Running Impossible Movement & Velocity Anomaly Detection..."
python3 src/analytics/speed_anomaly.py

echo ""
echo "[5/5] Running Route Similarity & Travel Correlation..."
python3 src/analytics/route_similarity.py

echo ""
echo "=========================================================="
echo "[✔] All 5 PySpark Analytics Jobs Completed Successfully!"
echo "=========================================================="
