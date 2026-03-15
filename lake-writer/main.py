import os
import time
import boto3
from confluent_kafka import Consumer, KafkaError
from datetime import datetime

# --- Configuration ---
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "redpanda:29092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "telemetry_stream")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "password123")
BUCKET_NAME = "landing-zone"

# --- S3 Client (MinIO) ---
s3_client = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    region_name="us-east-1",  # Required by boto3, even for local MinIO
)

# --- Kafka Consumer ---
conf = {
    "bootstrap.servers": KAFKA_BROKER,
    "group.id": "lake_writer_group",
    "auto.offset.reset": "earliest",
}
consumer = Consumer(conf)
consumer.subscribe([KAFKA_TOPIC])

print("🚀 Starting Lake Writer: Redpanda -> MinIO Landing Zone")

message_buffer = []
last_flush_time = time.time()
FLUSH_INTERVAL_SEC = 30
MAX_BATCH_SIZE = 500


def flush_to_s3():
    global message_buffer, last_flush_time
    if not message_buffer:
        return

    # Create an enterprise-grade partitioned path: telemetry/YYYY/MM/DD/batch_timestamp.json
    now = datetime.utcnow()
    s3_key = f"telemetry/{now.strftime('%Y/%m/%d')}/batch_{int(time.time())}.json"

    # Spark expects JSON Lines (NDJSON) - one JSON object per line
    file_content = "\n".join(message_buffer)

    # Upload directly to MinIO from memory using the new nested key
    s3_client.put_object(
        Bucket=BUCKET_NAME, Key=s3_key, Body=file_content.encode("utf-8")
    )

    print(f"🌊 Flushed {len(message_buffer)} records to s3://{BUCKET_NAME}/{s3_key}")

    message_buffer = []
    last_flush_time = time.time()


try:
    while True:
        msg = consumer.poll(timeout=1.0)

        # Check if it's time to flush based on time, even if we don't have new messages
        if time.time() - last_flush_time > FLUSH_INTERVAL_SEC:
            flush_to_s3()

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                print(f"❌ Kafka error: {msg.error()}")
            continue

        # Add the raw JSON string to our buffer
        message_buffer.append(msg.value().decode("utf-8"))

        # Check if it's time to flush based on batch size
        if len(message_buffer) >= MAX_BATCH_SIZE:
            flush_to_s3()

except KeyboardInterrupt:
    print("\n🛑 Shutting down lake writer... flushing final messages.")
    flush_to_s3()
finally:
    consumer.close()
