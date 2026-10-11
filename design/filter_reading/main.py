import glob
import os
import json
import time
from google.cloud import pubsub_v1
# Load GCP service account credentials
files = glob.glob("*.json")
if len(files) > 0:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]
project_id = os.environ.get("GCP_PROJECT", "dev-methods-tools")
topic_name = os.environ.get("TOPIC_NAME", "smartmeter-dabash")
subscription_id = os.environ.get("SUB_ID", "smartmeter-filter-sub-dabash")
print(f"[FilterReading] Starting service on Project: {project_id}, Topic: {topic_name}...")
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)
sub_filter = 'attributes.function="raw"'
# Callback to handle received raw readings
def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    try:
        data = json.loads(message.data.decode("utf-8"))
        print(f"[FilterReading] Received RAW message: {data}")
        # Check for missing/None measurements
        if data.get("pressure") is None or data.get("temperature") is None:
            print(f"[FilterReading] ? Dropping record with missing/None measurement: {data}")
        else:
            print(f"[FilterReading]  Valid record. Forwarding to 'filtered': {data}")
            publisher.publish(topic_path, json.dumps(data).encode("utf-8"), function="filtered")
    except Exception as e:
        print(f"[FilterReading] Error processing message: {e}")
    finally:
        message.ack()
# Create subscription with filter if it doesn't already exist
try:
    subscriber.create_subscription(
        request={"name": subscription_path, "topic": topic_path, "filter": sub_filter}
    )
    print(f"[FilterReading] Created subscription {subscription_path}")
except Exception:
    print(f"[FilterReading] Subscription {subscription_path} already exists")
streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"[FilterReading] Listening for RAW messages on {subscription_path}...")
try:
    streaming_pull_future.result()
except KeyboardInterrupt:
    streaming_pull_future.cancel()
