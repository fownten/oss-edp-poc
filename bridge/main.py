import os
import paho.mqtt.client as mqtt
from confluent_kafka import Producer

# configure from environment
MQTT_BROKER = os.environ.get("MQTT_BROKER", "localhost")
MQTT_PORT = int(os.environ.get("MQTT_PORT", 1883))
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "telemetry_stream")

print(f"🚀 Starting Python Bridge: MQTT({MQTT_BROKER}) -> Kafka({KAFKA_BROKER})")

kafka_producer = Producer({"bootstrap.servers": KAFKA_BROKER})


def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Kafka delivery failed: {err}")


def on_connect(client, userdata, flags, reason_code, properties=None):
    print("✅ Connected to Mosquitto! Subscribing to devices...")
    client.subscribe("edp/telemetry/#")


def on_message(client, userdata, msg):
    payload = msg.payload.decode("utf-8")
    device_id = msg.topic.split("/")[-1]

    print(f"🌉 Bridging data for [{device_id}]")

    kafka_producer.produce(
        topic=KAFKA_TOPIC,
        key=device_id.encode("utf-8"),
        value=payload,
        callback=delivery_report,
    )
    kafka_producer.poll(0)


# --- Execution ---
mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)

try:
    mqtt_client.loop_forever()
except KeyboardInterrupt:
    print("\n🛑 Shutting down bridge... flushing Kafka.")
    kafka_producer.flush()
