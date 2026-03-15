FROM bitnamilegacy/spark:3.5.1

USER root
RUN pip install --no-cache-dir pyspark==3.5.3

WORKDIR /app
COPY ingest.py /app/ingest.py

USER 1001
