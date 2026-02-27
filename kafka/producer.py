
from kafka import KafkaProducer
import json
import time
import os

# Configuration
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: v.encode('utf-8') 
)

topic_name = 'events_raw'
data_file = os.path.join(os.path.dirname(__file__), '..', 'data', 'events_dirty.json')

sent_count = 0
error_count = 0

with open(data_file, 'r', encoding='utf-8') as file:
    for line_num, line in enumerate(file, 1):
        line = line.strip()
        if not line:
            continue
        
        try:
            # Send raw line as-is (including malformed JSON)
            producer.send(topic_name, value=line)
            sent_count += 1
            print(f"[{sent_count}] Sent: {line[:80]}...")
            time.sleep(0.1)  # Small delay to simulate real-time streaming
            
        except Exception as e:
            error_count += 1
            print(f"Error sending line {line_num}: {e}")

producer.flush()
producer.close()
print(f"  - Total sent: {sent_count}")
print(f"  - Errors: {error_count}")