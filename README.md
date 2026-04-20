# 🌍 Enterprise Data Platform POC

This project is a complete, event-driven Data Engineering Proof of Concept (PoC) designed to simulate an enterprise-grade IoT ingestion pipeline for the energy sector (e.g., Solar Yield and Battery State of Charge). 

It demonstrates a decoupled architecture capable of handling high-velocity telemetry data from physical edge devices, buffering it fault-tolerantly, and persisting it for time-series analysis.

## 🏗️ Architecture Stack

* **The Edge Simulator (Go):** Highly concurrent goroutines simulating physical inverters. Publishes lightweight payloads via the Eclipse Paho MQTT driver.
* **The Ingestion Hub (Mosquitto):** A lightweight MQTT broker representing the edge-to-cloud gateway (mimicking Azure IoT Hub).
* **The Event Stream (Redpanda/Kafka):** A high-performance, Kafka-compatible message broker that provides extreme durability and backpressure handling (mimicking Azure Event Hubs).
* **The Stream Sink (Python):** A worker microservice (`confluent-kafka` & `psycopg2`) dedicated to continuously draining the Kafka topic and bulk-inserting records into the database.
* **The Database (TimescaleDB):** PostgreSQL enhanced with the Timescale extension. Data is written to an optimized time-series `Hypertable` for hyper-fast aggregations.
* **The Visualization Layer (Grafana):** A real-time dashboard directly querying the Hypertable to visualize live energy metrics.

## 🚀 Getting Started

### Prerequisites
* Docker & Docker Compose

### 1. Spin up the Infrastructure
From the root directory, build and launch the entire multi-container stack:
```bash
docker compose up -d --build
```

### 2. Verify the Services
Ensure all 6 containers are running successfully:
```bash
docker compose ps
```
You should see:
1.  `enwyse-db` (TimescaleDB)
2.  `enwyse-mqtt` (Mosquitto)
3.  `enwyse-kafka` (Redpanda)
4.  `enwyse-generator` (Go Edge Simulator)
5.  `enwyse-bridge` (Python MQTT-to-Kafka)
6.  `enwyse-timescale-writer` (Python Kafka-to-DB)
7.  `enwyse-grafana` (Visualization)
8.  `enwyse-redpanda-console` (Kafka UI)

### 3. Access the UIs
* **Grafana Dashboard:** `http://localhost:3000` (admin / admin)
* **Redpanda Console:** `http://localhost:8080` (Live view of the `telemetry_stream` topic)

## 🗄️ Database Setup (One-Time)
If starting from a fresh volume, initialize the TimescaleDB Hypertable by connecting to the database container:
```bash
docker exec -it enwyse-db psql -U postgres -d energy_db
```
Execute the schema creation:
```sql
CREATE TABLE sensor_telemetry (
    time TIMESTAMPTZ NOT NULL,
    device_id TEXT NOT NULL,
    solar_yield_kw DOUBLE PRECISION,
    battery_soc_pct DOUBLE PRECISION
);

-- Convert to Timescale Hypertable
SELECT create_hypertable('sensor_telemetry', 'time');
CREATE INDEX ix_device_time ON sensor_telemetry (device_id, time DESC);
```

## 🛑 Operations
To gracefully tear down the architecture and stop the data generation:
```bash
docker compose down
```
*(Note: To wipe the database and Grafana state entirely, use `docker compose down -v` to destroy the persistent volumes).*
