#!/usr/bin/env bash
# ==============================================================================
# TraceHunt - HDFS Data Ingestion Script
# Ingests raw movement and event datasets into distributed Hadoop HDFS storage.
# ==============================================================================

set -e

HDFS_BASE_DIR="/tracehunt/raw"
LOCAL_DATA_DIR="${1:-dataset/sample}"

echo "=========================================================="
echo " TraceHunt: Ingesting Data into Hadoop HDFS"
echo " Source Directory: ${LOCAL_DATA_DIR}"
echo " Target HDFS Path: ${HDFS_BASE_DIR}"
echo "=========================================================="

# Check if Hadoop binary exists in PATH
if ! command -v hdfs &> /dev/null; then
    echo "[!] Error: 'hdfs' command not found. Ensure HADOOP_HOME is set and added to PATH."
    exit 1
fi

echo "[*] Step 1: Checking HDFS connection & creating directory..."
hdfs dfs -mkdir -p ${HDFS_BASE_DIR}

echo "[*] Step 2: Uploading CSV datasets to HDFS..."
for file in people.csv locations.csv incidents.csv movement_events.csv transactions.csv; do
    if [ -f "${LOCAL_DATA_DIR}/${file}" ]; then
        echo "    -> Uploading ${file}..."
        hdfs dfs -put -f "${LOCAL_DATA_DIR}/${file}" "${HDFS_BASE_DIR}/${file}"
    else
        echo "    [?] Warning: ${LOCAL_DATA_DIR}/${file} not found, skipping."
    fi
done

echo ""
echo "[*] Step 3: Verifying HDFS contents:"
hdfs dfs -ls -h ${HDFS_BASE_DIR}

echo ""
echo "[*] Step 4: Inspecting block distribution across DataNodes:"
hdfs fsck ${HDFS_BASE_DIR}/movement_events.csv -files -blocks -locations

echo ""
echo "=========================================================="
echo "[✔] HDFS Ingestion Completed Successfully!"
echo "=========================================================="
