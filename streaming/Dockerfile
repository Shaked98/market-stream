# Spark 3.5.3 + Iceberg 1.7.1 + the Kafka source + Avro + S3A, baked for fast pod startup
# (no per-job Maven downloads). Mirrors the jar-baking pattern of
# spark-k8s/docker/spark-iceberg/Dockerfile, building from the PUBLIC apache/spark base so
# the local docker-compose stack and the cluster are both reproducible without GHCR access.
#
# Build (from the repo root, so jobs/ + schemas/ are in context):
#   docker build -t ghcr.io/shaked98/spark-market-stream:3.5.3-iceberg1.7.1 -f streaming/Dockerfile .
FROM apache/spark:3.5.3

ARG ICEBERG_VERSION=1.7.1
ARG SPARK_VERSION=3.5.3
ARG SCALA_VERSION=2.12
ARG KAFKA_CLIENTS_VERSION=3.4.1
ARG COMMONS_POOL2_VERSION=2.11.1
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_BUNDLE_VERSION=1.12.262
ARG MVN=https://repo1.maven.org/maven2

USER root

RUN set -eux; cd /opt/spark/jars; \
    # ── Iceberg (catalog + SQL extensions) and the AWS bundle (S3FileIO data path) ──
    curl -fSL -O "${MVN}/org/apache/iceberg/iceberg-spark-runtime-3.5_${SCALA_VERSION}/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_${SCALA_VERSION}-${ICEBERG_VERSION}.jar"; \
    curl -fSL -O "${MVN}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar"; \
    # ── Kafka source (Structured Streaming) ──
    curl -fSL -O "${MVN}/org/apache/spark/spark-sql-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar"; \
    curl -fSL -O "${MVN}/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar"; \
    curl -fSL -O "${MVN}/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar"; \
    curl -fSL -O "${MVN}/org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar"; \
    # ── Avro encode/decode (from_avro) ──
    curl -fSL -O "${MVN}/org/apache/spark/spark-avro_${SCALA_VERSION}/${SPARK_VERSION}/spark-avro_${SCALA_VERSION}-${SPARK_VERSION}.jar"; \
    # ── S3A (Hadoop FileSystem) for the streaming checkpoint location ──
    curl -fSL -O "${MVN}/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar"; \
    curl -fSL -O "${MVN}/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar"

# Bake the job + the message contract.
COPY streaming/jobs /opt/spark/jobs
COPY schemas /opt/spark/jobs/schemas
RUN chmod -R 0755 /opt/spark/jobs

# Back to the unprivileged Spark user (uid 185 in the official image).
USER 185
ENV SCHEMA_PATH=/opt/spark/jobs/schemas/quote.avsc
