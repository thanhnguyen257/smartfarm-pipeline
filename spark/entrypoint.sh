#!/bin/bash
set -e

echo "SPARK_MODE: $SPARK_MODE"

if [ "$SPARK_MODE" = "master" ]; then
    # exec /opt/spark/sbin/start-master.sh -h spark-master
    echo "Starting Spark Master..."
    /opt/spark/sbin/start-master.sh -h spark-master &

    echo "Starting Jupyter Notebook..."
    jupyter notebook \
        --ip=0.0.0.0 \
        --port=8889 \
        --no-browser \
        --allow-root \
        --NotebookApp.token='' \
        --NotebookApp.password='' \
        --NotebookApp.notebook_dir=/opt &

    echo "Both Spark Master and Jupyter started. Keeping container alive..."
    # Prevent container from exiting
    wait -n
elif [ "$SPARK_MODE" = "worker" ]; then
    exec /opt/spark/sbin/start-worker.sh spark://spark-master:7077
elif [ "$SPARK_MODE" = "history" ]; then
    mkdir -p /opt/spark/spark-events
    exec /opt/spark/sbin/start-history-server.sh
elif [ "$SPARK_MODE" = "job" ]; then
    exec /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    /opt/enrich_stream.py
fi
