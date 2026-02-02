FROM apache/airflow:2.8.1-python3.8

USER root

# Java 11 & Tools
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    openjdk-11-jre-headless \
    procps \
    curl \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# env variables
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV SPARK_VERSION=3.5.1
ENV HADOOP_VERSION=3

# spark driver
RUN curl -o spark.tgz https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && tar -xf spark.tgz \
    && mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark \
    && rm spark.tgz

# 5. configure spark env
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

COPY jars/*.jar $SPARK_HOME/jars/
RUN chmod 644 $SPARK_HOME/jars/*.jar
# ---------------------------------------

USER airflow

# Python modules
RUN pip install --no-cache-dir \
    pyspark==3.5.1 \
    cassandra-driver \
    mysql-connector-python \
    polars
