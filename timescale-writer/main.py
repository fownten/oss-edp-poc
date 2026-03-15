import os
import json
import psycopg2
from confluent_kafka import Consumer, KafkaError

# --- Configuration from Environment ---
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "redpanda:29092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "telemetry_stream")
DB_DSN = os.environ.get(
    "DB_DSN", "postgresql://postgres:password123@timescaledb:5432/energy_db"
)

print("🚀 Starting TimescaleDB Consumer...")

# --- 1. Connect to Database ---
conn = psycopg2.connect(DB_DSN)
conn.autocommit = True  # Automatically commit inserts
cursor = conn.cursor()

# --- 2. Configure Kafka Consumer ---
conf = {
    "bootstrap.servers": KAFKA_BROKER,
    "group.id": "timescale_writer_group",
    "auto.offset.reset": "earliest",  # If we restart, pick up where we left off!
}
kafka_consumer = Consumer(conf)
kafka_consumer.subscribe([KAFKA_TOPIC])

print("✅ Connected to Stream and DB! Listening for Redpanda messages...")

# --- 3. The Infinite Read Loop ---
try:
    while True:
        msg = kafka_consumer.poll(timeout=1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"❌ Kafka error: {msg.error()}")
                break

        # Decode the JSON payload
        payload = json.loads(msg.value().decode("utf-8"))

        # Insert into the Hypertable
        insert_query = """
            INSERT INTO sensor_telemetry (time, device_id, solar_yield_kw, battery_soc_pct)
            VALUES (NOW(), %s, %s, %s);
        """
        cursor.execute(
            insert_query,
            (
                payload["device_id"],
                payload["solar_yield_kw"],
                payload["battery_soc_pct"],
            ),
        )

        print(f"💾 Stored data for [{payload['device_id']}] in TimescaleDB")

except KeyboardInterrupt:
    print("\n🛑 Shutting down consumer...")
finally:
    kafka_consumer.close()
    cursor.close()
    conn.close()
