FROM python:3.10-slim

WORKDIR /app

# Install Java (required for Flink) and wget
RUN apt-get update && \
    apt-get install -y default-jre wget && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/default-java

# Install Python requirements
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Create directory for JARs and download them
RUN mkdir -p /opt/flink/jars && \
    wget -qO /opt/flink/jars/flink-sql-connector-kafka.jar https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar && \
    wget -qO /opt/flink/jars/flink-sql-connector-elasticsearch7.jar https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-elasticsearch7/3.0.1-1.17/flink-sql-connector-elasticsearch7-3.0.1-1.17.jar

# Copy the script
COPY enrichment_job.py /app/

CMD ["python", "enrichment_job.py"]
