1. What happens if the pipeline crashes after writing to the raw zone but before writing to the curated zone? How to avoid it?

   Impact
   
   Data written to the raw zone is persisted and will not be lost.
   No data is written to the curated and consumption zones, leading to data inconsistency across zones.
   The Kafka consumer offset is not committed. After a restart, the pipeline will re-consume the same messages, resulting in duplicate processing (at-least-once delivery semantics).

    Mitigation & Checkpoint Strategy
   
   Enable Spark Structured Streaming checkpointing.
   Checkpoints store offsets, query state, and intermediate results. In the event of a failure, the pipeline can resume exactly where it left off, preventing both duplicate processing and data gaps.

2. If the producer scales to 50,000 messages/second, what is the first bottleneck in the architecture? How to resolve it?
   
   First Bottleneck
   
   Kafka broker network and disk I/O: High write throughput can saturate the broker’s network or disk, limiting ingestion speed.
   Spark consumer backpressure: If Spark cannot process data fast enough, it will fall behind the producer, leading to growing Kafka lag.

   Solutions
   
    Kafka:
    ncrease the number of partitions to parallelize writes.
    Add more brokers to distribute the load.
    Tune producer settings (batch.size, linger.ms) to improve batching efficiency.
    Use SSDs for faster disk writes.

   Spark:
   Increase maxOffsetsPerTrigger to process more data per micro-batch.
   Enable backpressure to dynamically adjust consumption rate.
   Increase parallelism by adding more executors and cores.

3. Kafka as historical data store vs Parquet data lake: pros, cons, and use cases

 Kafka as the source of truth

 ✅ Pros: Low latency, real-time streaming, built-in exactly-once semantics, excellent for event-driven architectures.
 ❌ Cons: High storage costs for long-term retention, not optimized for ad-hoc queries or complex analytics, limited support for historical data exploration.
 Parquet data lake
 ✅ Pros: High compression, fast analytical queries via columnar pruning, support for partitioning, ideal for long-term storage and batch processing/ML workloads.
 ❌ Cons: Higher write latency, not designed for low-latency streaming ingestion, not suitable for real-time event processing.

 Use Cases
 Kafka: Real-time data ingestion, stream processing pipelines, event buses, and operational monitoring.
 Parquet Data Lake: Historical analytics, reporting, machine learning, data warehousing, and long-term data archiving.

4. A sensor sends abnormal values for 2 hours due to a fault. How to detect and isolate the data without deleting it?

 Detection
 
 Use Spark to flag anomalies with fixed thresholds (e.g., temperature > 35°C, humidity > 90%, pressure out of valid range) by adding an is_anomaly boolean field.
 Extend detection with sliding windows, consecutive anomaly counting, and alerts for sustained faults.
 
 Isolation (without deletion)
 
 Preserve all raw data in the raw zone as the source of truth.
 Mark records with an anomaly flag (e.g., quality = 'ok' or 'anomaly') in the curated/consumption zones instead of deleting them.
 Partition or tag the data so that downstream analysis can easily filter out or include anomalies as needed.
 
5. A new CO₂ sensor is added. Which files need to be modified？
 
 Files to modify
 producer.py: Add CO₂ value range, unit, generation logic, and anomaly rules.
 spark_pipeline.py:
 Extend the JSON schema to include the new co2 field.
 Update anomaly detection rules for CO₂.
 Adjust aggregation and transformation logic.
 analytics.py: Modify queries to support CO₂ metrics.
 api/app.py: Update endpoints (/sensors, /stats, /anomalies) to include CO₂ data.
 docs/: Update architecture diagrams, documentation, and rules to reflect the new sensor.
