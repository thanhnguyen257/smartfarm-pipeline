FROM apache/spark:3.5.7-scala2.12-java11-python3-ubuntu AS spark-base

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    vim \
    procps \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir requests 

# RUN curl -Lo $SPARK_HOME/jars/spark-sql-kafka-0-10_2.12-3.5.7.jar \
#     https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.7/spark-sql-kafka-0-10_2.12-3.5.7.jar && \
#     curl -Lo $SPARK_HOME/jars/kafka-clients-3.7.0.jar \
#     https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.7.0/kafka-clients-3.7.0.jar && \
#     curl -Lo $SPARK_HOME/jars/spark-token-provider-kafka-0-10_2.12-3.3.0.jar \
#     https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.3.0/spark-token-provider-kafka-0-10_2.12-3.3.0.jar && \
#     curl -Lo $SPARK_HOME/jars/commons-pool2-2.12.0.jar \
#     https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar && \

COPY spark/jars/ $SPARK_HOME/jars/

FROM spark-base AS spark-final

ENV PYSPARK_PYTHON=python3 \
    SPARK_HOME=/opt/spark \
    PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH \
    PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH \
    SPARK_NO_DAEMONIZE=true

WORKDIR $SPARK_HOME

COPY spark/conf/spark-defaults.conf $SPARK_HOME/conf/
COPY spark/jobs/ /opt/
RUN mkdir -p $SPARK_HOME/spark-events
RUN mkdir -p /opt/alert_logs

COPY spark/entrypoint.sh /opt/
RUN chmod +x /opt/entrypoint.sh

ENTRYPOINT ["/opt/entrypoint.sh"]
CMD ["bash"]
