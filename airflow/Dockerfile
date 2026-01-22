ARG AIRFLOW_IMAGE=apache/airflow:2.9.0
FROM ${AIRFLOW_IMAGE}

USER root
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
USER airflow

COPY docker/airflow/requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
