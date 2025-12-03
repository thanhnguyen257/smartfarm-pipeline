FROM apache/spark:3.5.7-scala2.12-java11-python3-ubuntu AS spark-base

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    vim \
    procps \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir jupyter requests 

EXPOSE 8889
ENV JUPYTER_PORT=8889

ENV PYSPARK_DRIVER_PYTHON=jupyter
ENV PYSPARK_DRIVER_PYTHON_OPTS="notebook --no-browser --allow-root --ip=0.0.0.0 --port=${JUPYTER_PORT} --NotebookApp.token='' --NotebookApp.password=''"

# RUN curl -Lo $SPARK_HOME/jars/spark-sql-kafka-0-10_2.12-3.5.7.jar \
#     https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.7/spark-sql-kafka-0-10_2.12-3.5.7.jar && \
#     curl -Lo $SPARK_HOME/jars/kafka-clients-3.7.0.jar \
#     https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.7.0/kafka-clients-3.7.0.jar

# COPY jars/spark-sql-kafka-0-10_2.12-3.5.7.jar $SPARK_HOME/jars/spark-sql-kafka-0-10_2.12-3.5.7.jar
# COPY jars/kafka-clients-3.7.0.jar $SPARK_HOME/jars/kafka-clients-3.7.0.jar
# COPY jars/commons-pool2-2.12.0.jar $SPARK_HOME/jars/commons-pool2-2.12.0.jar
# COPY jars/spark-streaming-kafka-0-10_2.12-3.3.0.jar $SPARK_HOME/jars/spark-streaming-kafka-0-10_2.12-3.3.0.jar
# COPY jars/spark-token-provider-kafka-0-10_2.12-3.3.0.jar $SPARK_HOME/jars/spark-token-provider-kafka-0-10_2.12-3.3.0.jar
COPY spark/jars/ $SPARK_HOME/jars/


# RUN curl -Lo $SPARK_HOME/jars/elasticsearch-spark-30_2.12-8.19.6.jar \
#     https://repo1.maven.org/maven2/org/elasticsearch/elasticsearch-spark-30_2.12/8.19.6/elasticsearch-spark-30_2.12-8.19.6.jar

# COPY jars/elasticsearch-spark-30_2.12-8.19.6.jar $SPARK_HOME/jars/elasticsearch-spark-30_2.12-8.19.6.jar

FROM spark-base AS spark-final

ENV PYSPARK_PYTHON=python3 \
    SPARK_HOME=/opt/spark \
    PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH \
    PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH \
    SPARK_NO_DAEMONIZE=true

WORKDIR $SPARK_HOME

COPY spark/conf/spark-defaults.conf $SPARK_HOME/conf/
COPY spark/enrich_stream.py /opt/
COPY data/locations.db /opt/
RUN mkdir -p $SPARK_HOME/spark-events

COPY spark/entrypoint.sh /opt/
RUN chmod +x /opt/entrypoint.sh

ENTRYPOINT ["/opt/entrypoint.sh"]
CMD ["bash"]
