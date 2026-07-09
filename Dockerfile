ARG FLINK_VERSION=2.0

FROM apache/flink:${FLINK_VERSION}-java21

SHELL ["/bin/bash", "-c"]

USER flink

WORKDIR /opt/flink

# Kafka connector
RUN echo "-> Install Kafka connector" && \
    curl -fLo /opt/flink/lib/flink-sql-connector-kafka.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/4.0.0-2.0/flink-sql-connector-kafka-4.0.0-2.0.jar

# Install Avro 
RUN echo "-> Install Avro Confluent format" && \
    curl -fLo /opt/flink/lib/flink-sql-avro-confluent-registry.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-avro-confluent-registry/2.0.0/flink-sql-avro-confluent-registry-2.0.0.jar