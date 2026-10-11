import glob
import os
import json
import time
from datetime import datetime
from google.cloud import pubsub_v1
# Load GCP service account credentials
files = glob.glob("*.json")
if len(files) > 0:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]
project_id = "dev-methods-tools"
topic_name = "smartmeter-dabash"
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)
print(f"--- Smart Meter Producer Simulator ---")
print(f"Target Topic: {topic_path}\n")
# Test batch: Includes valid readings and records with 'None'
test_readings = [
    {"meter_id": 101, "pressure": 101.32, "temperature": 25.0, "timestamp": datetime.utcnow().isoformat()},
    {"meter_id": 102, "pressure": None,   "temperature": 28.5, "timestamp": datetime.utcnow().isoformat()},  # Should be DROPPED
    {"meter_id": 103, "pressure": 98.45,  "temperature": 21.0, "timestamp": datetime.utcnow().isoformat()},
    {"meter_id": 104, "pressure": 105.10, "temperature": None, "timestamp": datetime.utcnow().isoformat()},  # Should be DROPPED
    {"meter_id": 105, "pressure": 102.75, "temperature": 32.0, "timestamp": datetime.utcnow().isoformat()},
    {"meter_id": 106, "pressure": 100.00, "temperature": 18.5, "timestamp": datetime.utcnow().isoformat()}
]
for reading in test_readings:
    data_str = json.dumps(reading)
    future = publisher.publish(topic_path, data_str.encode("utf-8"), function="raw")
    msg_id = future.result()
    print(f"[Producer] Published RAW reading -> {reading}")
    time.sleep(1)
print("\n--- All test readings successfully sent! ---")
