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
subscription_id = os.environ.get("SUB_ID", "smartmeter-convert-sub-dabash")
print(f"[ConvertReading] Starting service on Project: {project_id}, Topic: {topic_name}...")
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)
sub_filter = 'attributes.function="filtered"'
# Callback to handle received filtered readings
def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    try:
        data = json.loads(message.data.decode("utf-8"))
        print(f"[ConvertReading] Received FILTERED message: {data}")
        # Conversion formulas:
        # P(psi) = P(kPa) / 6.895
        # T(F) = T(C) * 1.8 + 32
        p_kpa = float(data["pressure"])
        t_c = float(data["temperature"])
        p_psi = round(p_kpa / 6.895, 2)
        t_f = round(t_c * 1.8 + 32, 2)
        data["pressure"] = p_psi
        data["temperature"] = t_f
        print(f"[ConvertReading] Converted (P: {p_psi} psi, T: {t_f} F). Forwarding to 'converted' -> BigQuery: {data}")
        publisher.publish(topic_path, json.dumps(data).encode("utf-8"), function="converted")
    except Exception as e:
        print(f"[ConvertReading] Error converting message: {e}")
    finally:
        message.ack()
# Create subscription with filter if it doesn't already exist
try:
    subscriber.create_subscription(
        request={"name": subscription_path, "topic": topic_path, "filter": sub_filter}
    )
    print(f"[ConvertReading] Created subscription {subscription_path}")
except Exception:
    print(f"[ConvertReading] Subscription {subscription_path} already exists")
streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"[ConvertReading] Listening for FILTERED messages on {subscription_path}...")
try:
    streaming_pull_future.result()
except KeyboardInterrupt:
    streaming_pull_future.cancel()
