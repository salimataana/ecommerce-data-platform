FROM python:3.12-slim-trixie

# Installer Java 21 pour PySpark 4
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-21-jre-headless && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PYTHONUNBUFFERED=1
ENV SPARK_LOCAL_HOSTNAME=localhost

WORKDIR /app

# Installer les dépendances Python
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Copier le code
COPY src/ ./src/

# Commande par défaut
CMD ["python", "-m", "src.ingestion.api_ingestion"]