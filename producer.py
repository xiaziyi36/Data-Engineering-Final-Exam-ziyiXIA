import time
import json
import random
import argparse
from kafka import KafkaProducer

SENSORS = ['temperature', 'humidity', 'pressure']

def generate_reading(sensor_type, source):
    # normal ranges
    ranges = {
        'temperature': (15.0, 45.0),
        'humidity': (30.0, 95.0),
        'pressure': (980.0, 1040.0)
    }
    units = {'temperature': 'C', 'humidity': '%', 'pressure': 'hPa'}
    
    is_anomaly = random.random() < 0.1
    min_v, max_v = ranges[sensor_type]
    
    if is_anomaly:
        # Generate out of bounds
        val = random.uniform(max_v + 1, max_v + 20) if random.choice([True, False]) else random.uniform(min_v - 20, min_v - 1)
    else:
        val = random.uniform(min_v, max_v)

    return {
        "sensor": sensor_type,
        "value": round(val, 2),
        "unit": units[sensor_type],
        "timestamp": int(time.time() * 1000),
        "source": source,
        "anomaly": is_anomaly
    }

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--count', type=int, default=100)
    parser.add_argument('--rate', type=int, default=10)
    parser.add_argument('--source', type=str, default='site-A-rack-12')
    args = parser.parse_args()

    # required reliability configs
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092', 'localhost:9093', 'localhost:9094'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        key_serializer=lambda x: x.encode('utf-8'),
        acks='all',
        retries=5,
        max_in_flight_requests_per_connection=1,
        linger_ms=5,
        batch_size=16384
    )

    topic = 'sensor-events'
    sent = 0
    delay = 1.0 / args.rate

    try:
        for _ in range(args.count):
            s_type = random.choice(SENSORS)
            payload = generate_reading(s_type, args.source)
            # partition by key
            producer.send(topic, key=s_type, value=payload)
            sent += 1
            if sent % 50 == 0:
                print(f"Sent {sent}/{args.count} events...")
            time.sleep(delay)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        producer.close()
        print(f"Finished sending {sent} events.")

if __name__ == '__main__':
    main()