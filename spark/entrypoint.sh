#!/bin/bash
set -e

echo "SPARK_MODE: $SPARK_MODE"

if [ "$SPARK_MODE" = "master" ]; then
    exec /opt/spark/sbin/start-master.sh -h spark-master
elif [ "$SPARK_MODE" = "worker" ]; then
    exec /opt/spark/sbin/start-worker.sh spark://spark-master:7077
elif [ "$SPARK_MODE" = "history" ]; then
    mkdir -p /opt/spark/spark-events
    exec /opt/spark/sbin/start-history-server.sh
elif [ "$SPARK_MODE" = "job" ]; then
    JOB_FILE=${SPARK_JOB_FILE:-/opt/enrich_stream.py}

    exec /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    "$JOB_FILE"
fi
