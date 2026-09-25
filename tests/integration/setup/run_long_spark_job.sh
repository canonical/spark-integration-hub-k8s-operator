#!/bin/bash

# Submit a driver that stays alive until killed externally (by deleting the
# driver pod). Runs in cluster mode so a driver pod exists, and returns
# immediately (waitAppCompletion=false) since the job never finishes on its own.
#   $1: username (service account)
#   $2: namespace

job_file="$(dirname "$0")/long_running_spark_job.py"

spark-client.spark-submit -v \
  --username $1 --namespace $2 \
  --deploy-mode cluster \
  --conf spark.kubernetes.submission.waitAppCompletion=false \
  --conf spark.executor.instances=1 \
  --conf spark.kubernetes.executor.request.cores=0.1 \
  "$job_file"
