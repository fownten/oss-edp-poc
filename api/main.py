import os
from fastapi import FastAPI, HTTPException, Query
import psycopg2
from psycopg2.extras import RealDictCursor

# --- Configuration ---
DB_DSN = os.environ.get(
    "DB_DSN", "postgresql://postgres:password123@timescaledb:5432/energy_db"
)

# --- Initialize FastAPI ---
app = FastAPI(
    title="Enwyse Telemetry API",
    description="REST API for downstream apps to query IoT sensor data.",
    version="1.0.0",
)


def get_db_connection():
    return psycopg2.connect(DB_DSN, cursor_factory=RealDictCursor)


@app.get("/api/v1/telemetry/{device_id}", summary="Get recent telemetry for a device")
def get_device_telemetry(
    device_id: str,
    limit: int = Query(10, description="Number of recent records to return", le=1000),
):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Query the TimescaleDB Hypertable
        query = """
            SELECT time, device_id, solar_yield_kw, battery_soc_pct
            FROM sensor_telemetry
            WHERE device_id = %s
            ORDER BY time DESC
            LIMIT %s;
        """
        cursor.execute(query, (device_id, limit))
        results = cursor.fetchall()

        cursor.close()
        conn.close()

        if not results:
            raise HTTPException(
                status_code=404, detail=f"No data found for device: {device_id}"
            )

        return results

    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
