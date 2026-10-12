# One image for the master, the workers and the driver, so all of them run the
# same Python and the same deadlock_detector package (RDD lambdas are
# unpickled on the workers, so they must be able to import it).
FROM python:3.12-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps time git \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s /usr/lib/jvm/java-17-openjdk-* /opt/java
ENV JAVA_HOME=/opt/java

WORKDIR /opt/deadlock-detector
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir ".[dev]"

ENV SPARK_HOME=/usr/local/lib/python3.12/site-packages/pyspark \
    SPARK_CONF_DIR=/opt/spark-conf \
    PYSPARK_PYTHON=/usr/local/bin/python3
COPY docker/conf /opt/spark-conf
