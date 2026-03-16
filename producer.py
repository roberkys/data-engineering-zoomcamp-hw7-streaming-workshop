import pandas as pd
from kafka import KafkaProducer
import json
from time import time
import numpy as np

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

df = pd.read_parquet('green_tripdata_2025-10.parquet')

columns = [
    'lpep_pickup_datetime',
    'lpep_dropoff_datetime',
    'PULocationID',
    'DOLocationID',
    'passenger_count',
    'trip_distance',
    'tip_amount',
    'total_amount'
]

df = df[columns]

# Convert datetimes to strings
df['lpep_pickup_datetime'] = df['lpep_pickup_datetime'].astype(str)
df['lpep_dropoff_datetime'] = df['lpep_dropoff_datetime'].astype(str)

# Handle NaNs: convert to None for JSON null
df = df.replace({np.nan: None})

t0 = time()

# Use to_dict('records') which is generally faster and handles NaNs as specified
for row_dict in df.to_dict('records'):
    producer.send('green-trips', value=row_dict)

producer.flush()

t1 = time()
print(f'took {(t1 - t0):.2f} seconds')
