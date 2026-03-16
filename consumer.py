from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'green-trips',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='homework-consumer',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    consumer_timeout_ms=5000  # Stop after 5 seconds of no messages
)

count = 0
for message in consumer:
    if message.value['trip_distance'] > 5.0:
        count += 1

print(f'Number of trips with trip_distance > 5.0: {count}')
