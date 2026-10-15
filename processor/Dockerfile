FROM flink:2.2.1-scala_2.12-java17

USER root
RUN apt-get update && apt-get install -y --no-install-recommends python3.12 python3.12-venv curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN python3.12 -m venv /opt/pyflink
ENV PATH="/opt/pyflink/bin:${PATH}" \
    PYFLINK_CLIENT_EXECUTABLE=/opt/pyflink/bin/python

COPY processor /opt/processor
RUN pip install --no-cache-dir /opt/processor

# The SQL connector is a fat JAR that also supplies the DataStream KafkaSource.
RUN curl --fail --location --silent --show-error \
    https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/5.0.0-2.2/flink-sql-connector-kafka-5.0.0-2.2.jar \
    --output /opt/flink/lib/flink-sql-connector-kafka-5.0.0-2.2.jar
