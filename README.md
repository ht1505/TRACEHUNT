# TRACEHUNT: Distributed Big Data Event & Movement Investigation System

[![Big Data Capstone](https://img.shields.io/badge/Course-Big%20Data%20Analytics%20(Sem%207)-blue.svg)]()
[![Hadoop](https://img.shields.io/badge/Hadoop-3.3.6%20HDFS-yellow.svg)](https://hadoop.apache.org/)
[![Spark](https://img.shields.io/badge/Apache%20Spark-3.5%20PySpark-orange.svg)](https://spark.apache.org/)
[![MongoDB](https://img.shields.io/badge/MongoDB-NoSQL-green.svg)](https://www.mongodb.com/)
[![Flask](https://img.shields.io/badge/Backend-Flask%20REST-lightgrey.svg)](https://flask.palletsprojects.com/)

> **A 4th-Year Undergraduate Big Data Analytics Project**  
> Demonstrating true distributed storage across Hadoop HDFS DataNodes, distributed processing with Apache Spark, low-latency NoSQL serving via MongoDB, and an interactive investigation dashboard.

---

## Table of Contents
1. [Project Overview](#1-project-overview)
2. [Problem Statement & Objectives](#2-problem-statement--objectives)
3. [Core Architecture & Topology](#3-core-architecture--topology)
4. [Technology Stack & Responsibilities](#4-technology-stack--responsibilities)
5. [Synthetic Dataset & Forensic Injections](#5-synthetic-dataset--forensic-injections)
6. [3-Node Physical Cluster Setup](#6-3-node-physical-cluster-setup)
7. [HDFS & Spark Configuration](#7-hdfs--spark-configuration)
8. [Distributed Spark Analytics Modules](#8-distributed-spark-analytics-modules)
9. [MongoDB Operational Layer](#9-mongodb-operational-layer)
10. [Flask API & Web Dashboard](#10-flask-api--web-dashboard)
11. [Running the Project (Step-by-Step)](#11-running-the-project-step-by-step)
12. [Examiner Demonstration Script & Viva Proofs](#12-examiner-demonstration-script--viva-proofs)
13. [Team Contributions (3-Student Division)](#13-team-contributions-3-student-division)
14. [Limitations & Future Scope](#14-limitations--future-scope)

---

## 1. Project Overview

In a modern metropolis, millions of spatial-temporal events are logged daily: metro station taps, transit passes, retail payments, and civic sensor records. When an emergency or forensic investigation occurs:
- Traditional single-node relational databases crash due to memory saturation and lack of distributed execution.
- **TraceHunt** provides a production-grade distributed Big Data pipeline where:
  1. Multi-million row movement logs are split into blocks and distributed across **Worker 1 and Worker 2 DataNodes** in **Hadoop HDFS**.
  2. **Apache Spark (PySpark)** parallelizes computation across workers using window functions, spatial proximity joins, and velocity aggregations.
  3. Pre-computed forensic results and case files are stored in **MongoDB** for sub-millisecond retrieval.
  4. A **Flask** backend drives a responsive **Investigation Dashboard** for forensic analysts.

---

## 2. Problem Statement & Objectives

### Problem Statement
How can investigators reconstruct citizen trajectories, discover spatial-temporal co-presence during incidents, and flag physically impossible anomalies across millions of transit records without relying on centralized bottlenecks?

### Core Objectives
1. **True Distributed Storage**: Raw event streams must reside physically across multiple DataNodes in HDFS.
2. **True Distributed Processing**: Apache Spark jobs must split tasks across Worker 1 and Worker 2 executors.
3. **Meaningful NoSQL Role**: MongoDB must store actionable analytical summaries (journeys, incidents, anomalies), avoiding redundant raw log duplication.
4. **Explainable 4th-Year Engineering**: No unnecessary enterprise bloat (Kafka, Kubernetes, microservices). Every line of code is explainable in an undergraduate viva.

---

## 3. Core Architecture & Topology

```
                         [ SYNTHETIC DATA GENERATOR ]
                      (Small: 100k, Med: 1M, Large: 10M)
                                       │
                                       ▼
                          [ HADOOP HDFS STORAGE LAYER ]
                            NameNode (Master Laptop)
                                       │
                      ┌────────────────┴────────────────┐
                      ▼                                 ▼
           DataNode (Worker 1)               DataNode (Worker 2)
           [Block 0, Block 2]                [Block 1, Block 3]
                      │                                 │
                      └────────────────┬────────────────┘
                                       │
                                       ▼
                          [ SPARK DISTRIBUTED COMPUTE ]
                           Spark Master (Master Laptop)
                                       │
                      ┌────────────────┴────────────────┐
                      ▼                                 ▼
           Spark Worker (Worker 1)           Spark Worker (Worker 2)
           [Tasks / Partitions]              [Tasks / Partitions]
                      │                                 │
                      └────────────────┬────────────────┘
                                       │
                 ┌─────────────────────┼─────────────────────┐
                 ▼                     ▼                     ▼
          [Journey Recon]       [Incident Search]     [Anomaly Detector]
          Window Lag Trajectory Proximity Filter      Crowd Surge & Velocity
                 │                     │                     │
                 └─────────────────────┼─────────────────────┘
                                       │
                                       ▼
                          [ NoSQL OPERATIONAL LAYER ]
                                   MongoDB
                       (journeys, incidents, anomalies)
                                       │
                                       ▼
                          [ APPLICATION REST API ]
                                    Flask
                                       │
                                       ▼
                          [ INVESTIGATION DASHBOARD ]
                                HTML5 / CSS3 / JS
```

### 3-Node Hardware / Daemon Allocation

| Node | Physical Machine | Hadoop Daemons | Spark Daemons | App Services |
|---|---|---|---|---|
| **Master** | Laptop 1 (`master`) | NameNode, SecondaryNameNode | Spark Master (`:7077`) | MongoDB (`:27017`), Flask (`:5000`) |
| **Worker 1** | Laptop 2 (`worker1`) | DataNode, NodeManager | Spark Worker / Executor | Compute & Storage Worker |
| **Worker 2** | Laptop 3 (`worker2`) | DataNode, NodeManager | Spark Worker / Executor | Compute & Storage Worker |

---

## 4. Technology Stack & Responsibilities

1. **Hadoop HDFS (3.3.6)**: Distributed, fault-tolerant raw storage. Splits multi-gigabyte CSVs into blocks replicated across worker hard drives.
2. **Apache Spark (3.5 PySpark)**: In-memory distributed compute engine. Parallelizes filtering, spatial calculations, window ordering, and joins.
3. **MongoDB (NoSQL v8.x)**: Flexible document store. Serves low-latency JSON documents to the web interface.
4. **Flask (Python 3.9+)**: Lightweight API bridge connecting the UI to MongoDB and Spark job launchers.
5. **Dashboard (HTML/CSS/JS)**: Responsive dark-mode interface with journey timelines and proximity tables.

---

## 5. Synthetic Dataset & Forensic Injections

### Dataset Streams
- **`people.csv`**: Master profiles (`person_id, full_name, age_group, occupation, home_area, device_type`).
- **`locations.csv`**: Coordinate registry (`location_id, location_name, location_type, area, latitude, longitude, capacity`).
- **`movement_events.csv`**: Core Big Data log (`event_id, person_id, timestamp, location_id, event_type, transport_mode, device_id`).
- **`transactions.csv`**: Digital payments (`transaction_id, person_id, timestamp, location_id, amount, merchant_type, payment_method`).
- **`incidents.csv`**: Forensic incident cases (`incident_id, timestamp, location_id, incident_type, severity, description`).

### Planted Forensic Patterns (Guaranteed Viva Demonstration)
1. **Crowd Surge Anomaly**: Location `L003` experiences a 5x surge above baseline capacity between 18:00 and 19:00.
2. **Impossible Movement**: Citizen `P00023` records an event at `L001` at 10:00:00 and another at `L050` (28 km away) at 10:04:30 ($\approx 370\text{ km/h}$, flagging teleportation).
3. **Incident Proximity**: Incident `INC_0001` at Grand Bazaar at 14:30:00 has exactly 4 citizens (`P00023`, `P00045`, `P00078`, `P00102`) detected within 150m and $\pm 15$ mins.
4. **Route Correlation**: Citizens `P00311` and `P00312` traverse identical sequences of 4 locations in lockstep.

---

## 6. 3-Node Physical Cluster Setup

### Step 1: Connect to Single Hotspot & Configure `/etc/hosts`
Connect Master, Worker 1, and Worker 2 to the same phone hotspot.  
On **all 3 machines**, add to `/etc/hosts`:
```text
192.168.43.10  master
192.168.43.11  worker1
192.168.43.12  worker2
```

### Step 2: Configure Passwordless SSH from Master
```bash
# On Master:
ssh-keygen -t rsa -P "" -f ~/.ssh/id_rsa
ssh-copy-id hadoop@worker1
ssh-copy-id hadoop@worker2

# Verify:
ssh worker1 hostname
ssh worker2 hostname
```

---

## 7. HDFS & Spark Configuration

Templates are provided in `config/hadoop-conf/` and `config/spark-conf/`.

- **`core-site.xml`**:
  ```xml
  <property>
      <name>fs.defaultFS</name>
      <value>hdfs://master:9000</value>
  </property>
  ```
- **`hdfs-site.xml`**:
  ```xml
  <property>
      <name>dfs.replication</name>
      <value>2</value>
  </property>
  <property>
      <name>dfs.blocksize</name>
      <value>33554432</value> <!-- 32 MB for test multi-block splits -->
  </property>
  ```
- **`workers`**:
  ```text
  worker1
  worker2
  ```

---

## 8. Distributed Spark Analytics Modules

Located in `src/analytics/`:
1. `journey_reconstruction.py`: Window partitioned by `person_id`, calculates Haversine distance, builds ordered timelines.
2. `incident_investigation.py`: Spatial-temporal filter around incident coordinates, ranks nearby people.
3. `crowd_anomaly.py`: Hourly aggregations, flags $\text{hourly\_count} > \mu + 2.5\sigma$.
4. `speed_anomaly.py`: Window `lag()` displacement and elapsed time, flags velocities $> 130\text{ km/h}$.
5. `route_similarity.py`: Grouped sequence overlap identifying travel companions.

---

## 9. MongoDB Operational Layer

MongoDB stores pre-computed insights:
- `people`: Master citizen documents.
- `locations`: Spatial venue coordinates.
- `journeys`: Reconstructed travel timelines and total mileage.
- `incidents`: Investigated cases with detected nearby individuals.
- `anomalies`: Flagged crowd surges and impossible speeds.

Initialize schema and indexes:
```bash
python3 mongodb/schema_init.py
python3 mongodb/seed_reference_data.py dataset/sample
```

---

## 10. Flask API & Web Dashboard

Start the application:
```bash
python3 backend/app.py
```
Open **`http://localhost:5000`** in any browser.

Features:
- **System Overview**: Live event counters and database connection status.
- **Person Journey Investigation**: Search any `person_id` (e.g. `P00023`) to view interactive chronological stops.
- **Incident Investigation**: Select `INC_0001` to view the incident summary and table of nearby detected individuals.
- **Anomaly Intelligence**: Filter between Impossible Speeds and Crowd Activity Spikes.
- **Cluster Live Status**: Terminal command cheat sheet for examiners.

---

## 11. Running the Project (Step-by-Step)

```bash
# 1. Activate Environment
source venv/bin/activate

# 2. Generate Synthetic Dataset (Small for fast test, Large for demo)
python3 dataset/generator/generate_all.py --scale small --out dataset/sample

# 3. Seed MongoDB Reference Metadata
python3 mongodb/schema_init.py
python3 mongodb/seed_reference_data.py dataset/sample

# 4. Ingest Raw Dataset into Hadoop HDFS
./src/ingestion/hdfs_uploader.sh dataset/sample

# 5. Execute PySpark Analytics Pipeline
./scripts/spark/run_all_analytics.sh

# 6. Launch Flask Web Dashboard
python3 backend/app.py
```

---

## 12. Examiner Demonstration Script & Viva Proofs

| Step | Command to Run on Terminal | What to Explain to the Examiner |
|---|---|---|
| **1. Prove Daemons** | `jps` | "Master runs NameNode and Spark Master. Workers run DataNode and Spark Worker." |
| **2. Prove 2 DataNodes** | `hdfs dfsadmin -report` | "HDFS explicitly reports Live DataNodes = 2." |
| **3. Prove Block Split** | `hdfs fsck /tracehunt/raw/movement_events.csv -files -blocks -locations` | "The file is physically split into blocks across worker1 and worker2." |
| **4. Prove Spark Execution** | Spark UI: `http://master:8080` | "Spark distributes tasks across executors on Worker 1 and Worker 2." |
| **5. Prove Dashboard** | Web Browser: `http://localhost:5000` | "The web dashboard queries MongoDB for real-time investigation records." |

---

## 13. Team Contributions (3-Student Division)

| Team Member | Module Assigned | GitHub Commits |
|---|---|---|
| **Student 1** | **Cluster, Storage & Ingestion Engineer** | - Dataset generator & planted patterns<br>- Hadoop HDFS XML configuration<br>- HDFS ingestion & block distribution verification script |
| **Student 2** | **Distributed Processing & Analytics Engineer** | - Spark standalone cluster setup<br>- PySpark session builder<br>- 5 Analytical modules (Journey, Incident, Surges, Velocity) |
| **Student 3** | **NoSQL, Backend API & Dashboard Engineer** | - MongoDB schema design & indexing<br>- Flask REST API endpoints<br>- Web investigation dashboard & cluster status UI |

---

## 14. Limitations & Future Scope

### Limitations
1. In a mobile hotspot setup, network bandwidth limits shuffle performance on multi-gigabyte datasets compared to enterprise Gigabit LAN.
2. The dataset is synthetic and does not represent real GPS drift or mobile cell-tower triangulation inaccuracies.

### Future Scope
1. Streaming ingestion using Apache Kafka for real-time sensor updates.
2. Geospatial spatial indexing using PostGIS or GeoJSON 2dsphere indexes.
3. Automated multi-hop transit path prediction.
