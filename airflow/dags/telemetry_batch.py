from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

default_args = {
    "owner": "enwyse-data-eng",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    "lakehouse_telemetry_ingestion",
    default_args=default_args,
    description="Batch process JSON telemetry from MinIO into Iceberg",
    schedule_interval=timedelta(minutes=5),  # Runs every 5 minutes!
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["enwyse", "spark", "iceberg"],
) as dag:
    # The DockerOperator spins up an isolated Spark container, runs the job, and destroys it.
    process_telemetry = DockerOperator(
        task_id="run_spark_iceberg_ingestion",
        image="bitnamilegacy/spark:3.5.1",
        container_name="airflow-spark-worker",
        api_version="auto",
        auto_remove=True,
        mount_tmp_dir=False,
        # We mount your local spark directory into the container
        mounts=[
            Mount(
                source="/home/hans/personal/repos/enwyse-telemetry-hub/spark",
                target="/app",
                type="bind",
            )
        ],
        command="spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1,org.apache.hadoop:hadoop-aws:3.3.4 /app/ingest.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="enwyse-telemetry-hub_default",  # Ensures Spark can resolve minio and nessie
        environment={
            "MINIO_ENDPOINT": "http://minio:9000",
            "MINIO_ACCESS_KEY": "admin",
            "MINIO_SECRET_KEY": "password123",
            "NESSIE_URI": "http://nessie:19120/api/v1",
        },
    )

    process_telemetry
