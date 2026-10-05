#!/usr/bin/env bash
# ==============================================================================
# TraceHunt - Cluster Health & Daemon Verification Script
# Run on Master to verify connectivity and daemon status across all 3 nodes.
# ==============================================================================

echo "=========================================================="
echo " TRACEHUNT: 3-Node Cluster Health Verification"
echo "=========================================================="

echo ""
echo "[*] 1. Checking Local Master Java Daemons (jps):"
jps | grep -E "NameNode|SecondaryNameNode|ResourceManager|Master|Jps" || echo "    No Hadoop/Spark master daemons detected."

echo ""
echo "[*] 2. Checking Worker Network Connectivity:"
for host in worker1 worker2; do
    if ping -c 1 -W 1 "${host}" &> /dev/null; then
        echo "    [✔] ${host}: Reachable"
    else
        echo "    [✘] ${host}: UNREACHABLE (Check /etc/hosts and Wi-Fi connection)"
    fi
done

echo ""
echo "[*] 3. Checking Hadoop HDFS Live DataNodes:"
if command -v hdfs &> /dev/null; then
    hdfs dfsadmin -report | grep -E "Live datanodes|Configured Capacity|Present Capacity|DFS Remaining" || true
else
    echo "    'hdfs' command not found in PATH."
fi

echo ""
echo "[*] 4. Checking MongoDB Status (Port 27017):"
if nc -z -w 1 localhost 27017 &> /dev/null; then
    echo "    [✔] MongoDB is listening on port 27017."
else
    echo "    [!] MongoDB is NOT running or listening on port 27017."
fi

echo ""
echo "[*] 5. Checking Flask Web Dashboard API (Port 5050):"
if nc -z -w 1 localhost 5050 &> /dev/null; then
    echo "    [✔] Flask API is running on port 5050 (http://localhost:5050)."
else
    echo "    [!] Flask API is NOT running on port 5050."
fi

echo ""
echo "=========================================================="
