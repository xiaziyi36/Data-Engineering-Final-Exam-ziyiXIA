from flask import Flask, jsonify, request
import os
import json
import pandas as pd
from kafka import KafkaProducer

app = Flask(__name__)

# 【修正处】往上跳三层：api -> src -> root
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
LAKE_PATH = os.path.join(BASE_DIR, "tmp", "datalake")

try:
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
except:
    producer = None

SENSORS = ['temperature', 'humidity', 'pressure']

@app.route('/api/v1/health', methods=['GET'])
def health_check():
    return jsonify({"status": "UP"}), 200

@app.route('/api/v1/sensors', methods=['GET'])
def list_sensors():
    return jsonify({"sensors": SENSORS}), 200

@app.route('/api/v1/sensors/<sensor_type>/stats', methods=['GET'])
def get_stats(sensor_type):
    if sensor_type not in SENSORS:
        return jsonify({"error": "Invalid sensor type"}), 404
    
    # 优先读 consumption 聚合层，如果没有就读 curated 明细层
    path = os.path.join(LAKE_PATH, "consumption")
    if not os.path.exists(path):
        path = os.path.join(LAKE_PATH, "curated")

    try:
        df = pd.read_parquet(path)
        df_sensor = df[df['sensor'] == sensor_type]
        if df_sensor.empty:
             return jsonify({"error": "No data found"}), 404
        
        # 取前5条展示
        stats = df_sensor.head(5).to_dict(orient='records')
        # 转换不可序列化的对象
        for s in stats:
            for k, v in s.items():
                if not isinstance(v, (str, int, float, bool, list, dict, type(None))):
                    s[k] = str(v)
            
        return jsonify({"data": stats, "sensor": sensor_type}), 200
    except Exception as e:
        return jsonify({"error": str(e), "checked_path": path}), 500

if __name__ == '__main__':
    print(f"Server Root: {BASE_DIR}")
    app.run(debug=True, port=5000)