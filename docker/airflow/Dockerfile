FROM apache/airflow:3.1.1-python3.12

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_LOCAL_HOSTNAME=localhost
ENV PYTHONUNBUFFERED=1

USER airflow

COPY --chown=airflow:root requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir \
    "SQLAlchemy>=2.0,<2.1" \
    -r /tmp/requirements.txt \
    dbt-postgres==1.9.0 \
    && pip check