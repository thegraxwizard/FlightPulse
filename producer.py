import pandas as pd
from kafka import KafkaProducer
import json
import time

# --- 1. Create the Kafka Producer (UPDATED LINE) ---
# We are using the internal Docker service name 'kafka' and its internal port 29092
producer = KafkaProducer(
    bootstrap_servers='kafka:9092', # <-- CHANGE THIS LINE
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# ... (the rest of the code is the same) ...
topic_name = 'flight_events'
df = pd.read_csv('flights.csv')
print("Starting to stream flight data...")
for index, row in df.iterrows():
    message = row.to_dict()
    producer.send(topic_name, value=message)
    print(f"Sent: {message}")
    time.sleep(1)
print("Streaming complete.")
producer.flush()