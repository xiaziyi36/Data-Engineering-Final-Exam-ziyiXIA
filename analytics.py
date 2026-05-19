import os
import sys
import time

# 彻底解决 Windows 环境下的路径和权限问题
os.environ['HADOOP_HOME'] = 'C:\\hadoop'
os.environ['PATH'] = 'C:\\hadoop\\bin;' + os.environ['PATH']

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, date_format

def main():
    # 自动获取当前项目的绝对路径，防止 Spark 找错地方
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    # 将 Windows 的反斜杠 \ 替换为 Spark 喜欢的正斜杠 /
    curated_path = os.path.join(base_dir, "tmp", "datalake", "curated").replace("\\", "/")
    output_dir = os.path.join(base_dir, "outputs", "analytics").replace("\\", "/")
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    spark = SparkSession.builder \
        .appName("IoT_Analytics") \
        .master("local[*]") \
        .config("spark.hadoop.fs.permissions.umask-mode", "000") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    print(f"Loading data from: {curated_path}")
    
    try:
       
        curated_df = spark.read.parquet(f"file:///{curated_path}")
    except Exception as e:
        print(f"\n[ERROR] 读取失败！具体原因: {str(e)}")
        print("请检查该文件夹下是否有 .parquet 文件: " + curated_path)
        return

    # Q1: Top 5 hours with most anomalies
    print("\n--- Q1: Top 5 hours with highest anomalies ---")
    q1 = curated_df.filter(col("is_anomaly") == True) \
        .withColumn("h", hour(col("event_time"))) \
        .groupBy("h").count().orderBy(col("count").desc()).limit(5)
    q1.show()
    q1.toPandas().to_csv(os.path.join(output_dir, "q1_top_hours.csv"), index=False)

    # Q2: Global stats per sensor
    print("\n--- Q2: Global stats per sensor ---")
    curated_df.createOrReplaceTempView("curated")
    q2 = spark.sql("""
        SELECT sensor, 
               AVG(value) as mean_val, 
               MIN(value) as min_val, 
               MAX(value) as max_val, 
               STDDEV(value) as std_dev,
               (SUM(CAST(is_anomaly AS INT)) / COUNT(*)) * 100 as anomaly_rate_pct
        FROM curated
        GROUP BY sensor
    """)
    q2.show()
    q2.toPandas().to_csv(os.path.join(output_dir, "q2_global_stats.csv"), index=False)

    # Q3: Daily evolution of temperature
    print("\n--- Q3: Daily evolution for Temperature ---")
    q3 = spark.sql("""
        SELECT DATE(event_time) as dt, 
               AVG(value) as daily_mean, 
               SUM(CAST(is_anomaly AS INT)) as anomaly_count
        FROM curated
        WHERE sensor = 'temperature'
        GROUP BY DATE(event_time)
        ORDER BY dt
    """)
    q3.show()
    q3.toPandas().to_csv(os.path.join(output_dir, "q3_temp_daily.csv"), index=False)

    # Q4: Partition pruning demo
    print("\n--- Q4: Partition Pruning Demonstration ---")
    start_time = time.time()
    spark.sql("SELECT count(*) FROM curated WHERE value > 0").collect()
    t1 = time.time() - start_time
    
    start_time = time.time()
    # 自动获取数据的年份，确保查询能命中分区
    latest_year = curated_df.select(date_format("event_time", "yyyy")).distinct().collect()[0][0]
    spark.sql(f"SELECT count(*) FROM curated WHERE sensor = 'temperature' AND event_year = {latest_year} AND value > 0").collect()
    t2 = time.time() - start_time

    print(f"Time without partition pruning: {t1:.4f} seconds")
    print(f"Time WITH partition pruning: {t2:.4f} seconds")
    print(f"Speedup factor: {t1/t2:.2f}x" if t2 > 0 else "Speedup: N/A")
    
    with open(os.path.join(output_dir, "q4_pruning_report.txt"), "w") as f:
        f.write(f"Without pruning: {t1:.4f}s\nWith pruning: {t2:.4f}s\n")

    print(f"\n[SUCCESS] 所有分析完成，CSV结果已存入: {output_dir}")
    spark.stop()

if __name__ == "__main__":
    main()