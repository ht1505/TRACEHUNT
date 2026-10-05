# TRACEHUNT: Comprehensive Viva Preparation Guide
## Top 20 Big Data Questions & Model Answers for 4th-Year Examiners

---

### Part 1: Distributed Storage (Hadoop HDFS)

#### Q1: What is Hadoop HDFS and what roles do your 3 laptops play?
**Answer:**
Hadoop Distributed File System (HDFS) is a distributed file system designed to store very large datasets reliably across commodity hardware. In our 3-node cluster:
- **Master Laptop**: Runs the **NameNode** (manages directory namespace, block metadata, and locations) and **SecondaryNameNode** (checkpoints the `fsimage` and `edits` log).
- **Worker 1 & Worker 2 Laptops**: Run **DataNodes**. They physically store the actual data blocks on their hard drives and send regular heartbeats to the NameNode.

#### Q2: What is the block size and replication factor in your project?
**Answer:**
- **Block Size**: Standard HDFS default is 128 MB. In our demonstration configuration, we tuned `dfs.blocksize` to 32 MB in `hdfs-site.xml` so that even our moderate-sized test files are physically split into multiple blocks across Worker 1 and Worker 2.
- **Replication Factor**: We set `dfs.replication` to **2** because we have exactly 2 worker DataNodes. Every block is stored on both workers, ensuring zero data loss if one worker temporarily drops off the network.

#### Q3: How do you prove to the examiner that data is actually distributed?
**Answer:**
We run the HDFS File System Check command:
```bash
hdfs fsck /tracehunt/raw/movement_events.csv -files -blocks -locations
```
This command outputs the exact block IDs and shows their physical IP/hostname locations (e.g., `Block 0 -> worker1, worker2`). We also show `hdfs dfsadmin -report`, which confirms **Live datanodes: 2**.

#### Q4: Why don't we store small files in HDFS?
**Answer:**
HDFS is optimized for large streaming reads of large files. Every file, directory, and block in HDFS consumes approximately 150 bytes of memory in the NameNode's RAM. Millions of small files would exhaust NameNode memory without storing much actual data (the "small file problem").

---

### Part 2: Distributed Processing (Apache Spark & PySpark)

#### Q5: Why did you use Apache Spark instead of traditional Hadoop MapReduce?
**Answer:**
MapReduce writes all intermediate states to physical disk between Map and Reduce phases, causing high I/O latency. Apache Spark processes data **in-memory** using resilient distributed datasets (RDDs) and DataFrames, making iterative queries, window functions, and multi-stage joins up to 10 to 100 times faster.

#### Q6: What is the difference between a Transformation and an Action in Spark?
**Answer:**
- **Transformation**: A lazy operation that defines a new DataFrame from an existing one without immediately computing it (e.g., `filter()`, `join()`, `groupBy()`, `withColumn()`). Spark merely builds an execution lineage graph (DAG).
- **Action**: An operation that triggers the actual execution across the worker cluster and returns a value or writes to storage (e.g., `collect()`, `count()`, `show()`, `write()`).

#### Q7: How does your project run across the two workers?
**Answer:**
When submitting jobs with `--master spark://master:7077`:
1. The Spark Master allocates Executors on Worker 1 and Worker 2.
2. The dataset partitions in HDFS are read by the respective worker executors based on data locality.
3. The Spark Web UI (`http://master:8080`) visually demonstrates tasks running in parallel across both workers.

#### Q8: What is a Broadcast Join and where did you use it?
**Answer:**
In `incident_investigation.py` and `journey_reconstruction.py`, we join large movement logs (millions of rows) with small reference tables (`locations.csv` with 50 rows). Instead of shuffling both tables across the network, Spark broadcasts the small location table to all executors in memory, eliminating network shuffle overhead.

---

### Part 3: Analytical Logic & Seeded Patterns

#### Q9: How did your PySpark job detect the Crowd Surge Anomaly?
**Answer:**
1. We group events by `location_id` and 1-hour time windows:
   ```python
   df.groupBy("location_id", "hour_bucket").agg(count("event_id").alias("hourly_events"))
   ```
2. We compute the baseline average ($\mu$) and standard deviation ($\sigma$) per location.
3. Any hour where $\text{hourly\_events} > \mu + 2.5 \times \sigma$ and exceeds capacity threshold is flagged as a statistical spike and saved to MongoDB.

#### Q10: How did your system detect the Impossible Movement (Speed Anomaly)?
**Answer:**
1. We partition events by `person_id` ordered by `timestamp` using a Spark Window:
   ```python
   win = Window.partitionBy("person_id").orderBy("timestamp")
   ```
2. Using `lag()`, we extract the citizen's previous location coordinates $(lat_1, lon_1)$ and timestamp $t_1$.
3. We calculate displacement $\Delta d$ via the Haversine formula and time difference $\Delta t = t_2 - t_1$.
4. Speed is $v = \Delta d / \Delta t$. If $v > 130\text{ km/h}$ for ground transit, it is flagged as impossible movement / teleportation.

---

### Part 4: NoSQL (MongoDB) & Backend Architecture

#### Q11: Why is MongoDB needed? Why not keep everything in HDFS or MySQL?
**Answer:**
- **Why not HDFS?** HDFS is an append-only distributed storage system designed for batch processing, not low-latency random key-value lookups. Querying a single person's journey from HDFS would take 30+ seconds.
- **Why not MySQL?** Processed journeys have variable nested structures (an array of timeline stops with coordinates and timestamps). MongoDB stores these natively as JSON/BSON documents.
- **Architecture Role**: HDFS is the **Data Lake** for raw massive logs; MongoDB is the **Operational Serving Layer** providing sub-5ms query responses to the Flask API.

#### Q12: What MongoDB collections exist in TraceHunt?
**Answer:**
1. `people`: Master citizen profiles.
2. `locations`: Spatial coordinates and venue metadata.
3. `journeys`: Reconstructed chronological timelines and distances.
4. `incidents`: Investigated cases with ranked nearby people.
5. `anomalies`: Flagged crowd surges and speed violations.

---

### Part 5: Cluster Demonstration Cheat Sheet

| Step | Command to Run on Master | What to Explain to the Examiner |
|---|---|---|
| **1. Prove Daemons** | `jps` | "Here you see NameNode and SparkMaster on Master. On Worker 1 and Worker 2, running `jps` shows DataNode and SparkWorker." |
| **2. Prove 2 DataNodes** | `hdfs dfsadmin -report` | "HDFS explicitly reports Live DataNodes = 2 with active capacity across both laptops." |
| **3. Prove Block Split** | `hdfs fsck /tracehunt/raw/movement_events.csv -files -blocks -locations` | "The file is physically partitioned into blocks across worker1 and worker2." |
| **4. Prove Spark Execution** | `python3 src/analytics/incident_investigation.py` | "PySpark is reading from HDFS, computing spatial proximity across workers, and saving to MongoDB." |
| **5. Prove Web Dashboard** | Open `http://localhost:5000` | "The Flask interface visualizes the completed Big Data investigation in real time." |
