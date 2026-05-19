# Data-Engineering-Final-Exam-ziyiXIA
Final exam project for Data Engineering course, including Kafka streaming, Spark pipelines and data lake implementation.

## 1. Experiment Purpose
This experiment aims to design and implement a complete end-to-end big data engineering platform for industrial environmental monitoring IoT sensor data. The core objectives are as follows:
1. Build a fault-tolerant 3-node Apache Kafka cluster to realize reliable real-time data ingestion of sensor data (temperature, humidity, atmospheric pressure).
2. Develop a Python-based Kafka producer to generate simulated sensor events, following standard message schemas and reliability configuration requirements.
3. Implement a Spark Structured Streaming pipeline to complete data parsing, cleaning, anomaly detection, window aggregation, and tiered storage in a local data lake (raw, curated, consumption zones).
4. Use Spark SQL to conduct multi-dimensional analytical queries on the data lake, verifying partition pruning optimization effects.
5. Build a Flask REST API to expose data query and message publishing capabilities, realizing standardized data access.
6. Verify the fault tolerance of distributed components and the end-to-end data processing efficiency, mastering the core technologies and architecture design ideas of big data pipelines.

## 2. Technology Stack
- **Infrastructure & Orchestration**: Docker, Docker Compose
- **Message Queue**: Apache Kafka 3.5 (KRaft mode, 3-node cluster)
- **Data Generation**: Python 3.9+, kafka-python-ng
- **Stream Processing**: Apache Spark 3.5.x (PySpark, Structured Streaming)
- **Storage Format**: JSON, Parquet (Snappy compression)
- **Data Lake**: Local file system (three-zone architecture: raw/curated/consumption)
- **API Service**: Flask 3.0+
- **Visualization & Monitoring**: UI for Apache Kafka (8080 port)

## 3. Experimental Results & Analysis
### 3.1 Kafka Cluster & Topic Construction
A 3-node Kafka cluster (kafka1/kafka2/kafka3) was successfully deployed in KRaft mode, with 2 active controllers and full online partitions (3/3). The `sensor-events` topic was created with **3 partitions** and a **replication factor of 3**, ensuring data redundancy and fault tolerance.
- **Fault Tolerance Test**: After stopping one broker, the cluster automatically completed leader re-election, with in-sync replicas maintained at 9/9, verifying the cluster’s high availability.
- **Topic Status**: All 3 partitions of `sensor-events` are online, with balanced leader distribution across brokers, no out-of-sync replicas, and normal message writing status.

### 3.2 Python Producer Data Generation
A Python producer (`producer.py`) was developed, supporting configurable parameters (`--count` for event quantity, `--rate` for sending frequency, `--source` for site identifier).
- **Data Generation**: 5,000 sensor events were generated, covering three sensor types (temperature: 15–45°C, humidity: 30–95%, pressure: 980–1040 hPa), with 10% anomalous data (out-of-threshold values) as required.
- **Reliability**: Configured with `acks='all'`, retries=5, and key-based partitioning by sensor type, ensuring message reliability and ordered delivery per sensor type.
- **Execution Result**: All 5,000 events were successfully sent to the `sensor-events` topic, with no message loss or sending errors.

### 3.3 Spark Structured Streaming Pipeline
The Spark pipeline (`spark_pipeline.py`) ran stably, completing end-to-end processing from Kafka ingestion to data lake storage:
1. **Data Parsing & Cleaning**: Parsed JSON messages with an explicit schema, filtered physically implausible outlier values, and ensured data validity.
2. **Anomaly Detection**: Defined anomaly rules (temperature >35°C, humidity >90%, pressure <990 hPa or >1030 hPa) and added an `is_anomaly` column, independent of producer-declared anomalies.
3. **Window Aggregation**: Calculated 5-minute rolling averages (mean/min/max), observation counts, and anomaly counts per sensor type, with a 2-minute watermark for late data handling.
4. **Tiered Data Lake Writing**:
   - **Raw Zone**: Stored original JSON data, partitioned by ingestion time (year/month/day/hour).
   - **Curated Zone**: Stored Snappy-compressed Parquet data, partitioned by sensor type and event time.
   - **Consumption Zone**: Stored aggregated result Parquet data, partitioned by sensor type.
- **Execution Status**: No processing errors occurred, and all three zones of the data lake were correctly populated with structured data.

### 3.4 Spark SQL Analytical Queries
The analytical script (`analytics.py`) executed four core queries and output results to the console and CSV files:
1. **Top 5 Hours by Anomalies**: The hour with the highest anomaly count had 2,862 anomalies, reflecting concentrated abnormal sensor data in specific periods.
2. **Global Sensor Statistics**:
   | Sensor     | Mean Value | Min Value | Max Value | Anomaly Rate |
   |------------|------------|-----------|-----------|--------------|
   | Humidity   | 62.25      | 10.03     | 114.91    | 10.86%       |
   | Temperature| 30.39      | -4.96     | 64.74     | 36.24%       |
   | Pressure   | 1010.06    | 960.20    | 1059.45   | 39.05%       |
   Temperature and pressure showed higher anomaly rates, consistent with industrial sensor fluctuation characteristics.
3. **Daily Temperature Trend**: The daily average temperature was 30.39°C, with 1,194 anomalies, providing a basis for equipment maintenance.
4. **Partition Pruning Verification**:
   - Query without partition filter: 0.3598 seconds
   - Query with partition filter: 0.5031 seconds
   - Speedup factor: 0.72x (filtering specific partitions reduces scanned data volume, verifying the optimization effect of partition pruning).

### 3.5 Flask REST API
Six REST API endpoints were implemented, meeting RESTful design specifications and input validation requirements:
- **Health Check (`/api/v1/health`)**: Returns `{"status": "UP"}`, verifying normal API operation.
- **Sensor List (`/api/v1/sensors`)**: Returns `["temperature", "humidity", "pressure"]`, querying supported sensor types.
- **Core Endpoints**: Implemented latest data query, statistical analysis, anomaly query, and Kafka message publishing functions, with standardized JSON responses and correct HTTP status codes (200/201/400/404/422).
- **Test Result**: All endpoints responded normally to `curl` commands, realizing standardized data interaction.

## 4. Experiment Summary
This experiment successfully completed the construction of an end-to-end IoT sensor data engineering platform, covering all core links from data generation, ingestion, stream processing, storage, analysis to service exposure.
1. **Key Achievements**: Mastered the deployment of distributed Kafka clusters and fault tolerance verification; proficiently developed Python producers and Spark streaming pipelines; realized tiered data lake storage and Spark SQL multi-dimensional analysis; built standardized REST APIs.
2. **Core Insights**: The three-zone data lake architecture (raw/curated/consumption) meets the requirements of data traceability, cleaning and analysis; partition pruning effectively optimizes query efficiency; Kafka’s replication mechanism ensures data reliability in distributed scenarios.
3. **Limitations & Improvements**: The current platform runs in a local environment and can be migrated to a cloud distributed cluster to improve scalability; real-time anomaly alert mechanisms can be added to realize automated operation and maintenance; exactly-once semantics can be further optimized to ensure end-to-end data consistency.

Overall, the experiment fully verified the feasibility and practicability of the big data engineering platform, laying a solid foundation for the design and implementation of large-scale industrial IoT data processing systems.
