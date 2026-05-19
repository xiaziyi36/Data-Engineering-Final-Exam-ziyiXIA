import os
import sys

# 彻底解决 Windows 权限报错的强制配置
os.environ['HADOOP_HOME'] = 'C:\\hadoop'
os.environ['PATH'] = 'C:\\hadoop\\bin;' + os.environ['PATH']

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, current_timestamp, 
    year, month, dayofmonth, hour, from_unixtime, when
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, BooleanType

def main():
    spark = SparkSession.builder \
        .appName("IoT_Sensor_Pipeline") \
        .master("local[*]") \
        .config("spark.sql.streaming.checkpointLocation", "./tmp/checkpoints") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([
        StructField("sensor", StringType(), True),
        StructField("value", DoubleType(), True),
        StructField("unit", StringType(), True),
        StructField("timestamp", LongType(), True),
        StructField("source", StringType(), True),
        StructField("anomaly", BooleanType(), True)
    ])

    # 1. Read Kafka
    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094") \
        .option("subscribe", "sensor-events") \
        .option("startingOffsets", "earliest") \
        .load()

    # 2. Parsing
    parsed_df = df_kafka.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

    # Physical plausibility filter
    valid_df = parsed_df.filter(
        (col("value").isNotNull()) & 
        (col("value") > -100) & (col("value") < 2000)
    )

    # 3. Validation & Anomaly Detection
    enriched_df = valid_df.withColumn(
        "event_time", from_unixtime(col("timestamp") / 1000).cast("timestamp")
    ).withColumn(
        "is_anomaly",
        when((col("sensor") == "temperature") & (col("value") > 35.0), True)
        .when((col("sensor") == "humidity") & (col("value") > 90.0), True)
        .when((col("sensor") == "pressure") & ((col("value") < 990.0) | (col("value") > 1030.0)), True)
        .otherwise(False)
    )

    # 4. Data Lake: RAW zone
    raw_df = enriched_df.withColumn("ingest_time", current_timestamp()) \
        .withColumn("ingest_year", year(col("ingest_time"))) \
        .withColumn("ingest_month", month(col("ingest_time"))) \
        .withColumn("ingest_day", dayofmonth(col("ingest_time"))) \
        .withColumn("ingest_hour", hour(col("ingest_time")))

    raw_query = raw_df.writeStream \
        .format("json") \
        .option("path", "./tmp/datalake/raw") \
        .option("checkpointLocation", "./tmp/checkpoints/raw") \
        .partitionBy("ingest_year", "ingest_month", "ingest_day", "ingest_hour") \
        .outputMode("append") \
        .start()

    # 5. Data Lake: CURATED zone
    curated_df = enriched_df.withColumn("event_year", year(col("event_time"))) \
        .withColumn("event_month", month(col("event_time"))) \
        .withColumn("event_day", dayofmonth(col("event_time")))

    curated_query = curated_df.writeStream \
        .format("parquet") \
        .option("compression", "snappy") \
        .option("path", "./tmp/datalake/curated") \
        .option("checkpointLocation", "./tmp/checkpoints/curated") \
        .partitionBy("sensor", "event_year", "event_month", "event_day") \
        .outputMode("append") \
        .start()

    # 6. Aggregation & CONSUMPTION zone
    watermarked_df = enriched_df.withWatermark("event_time", "2 minutes")

    agg_df = watermarked_df.groupBy(
        window(col("event_time"), "5 minutes"),
        col("sensor")
    ).agg(
        {"value": "avg", "value": "min", "value": "max", "sensor": "count"}
    ).withColumnRenamed("avg(value)", "mean") \
     .withColumnRenamed("min(value)", "min") \
     .withColumnRenamed("max(value)", "max") \
     .withColumnRenamed("count(sensor)", "obs_count")

    consumption_query = agg_df.writeStream \
        .format("parquet") \
        .option("path", "./tmp/datalake/consumption") \
        .option("checkpointLocation", "./tmp/checkpoints/consumption") \
        .partitionBy("sensor") \
        .outputMode("append") \
        .start()

    print("Spark Streaming Pipeline Started. Press Ctrl+C to stop.")
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()