#!/bin/bash
# Copyright 2026 Canonical Limited
# See LICENSE file for licensing details.

spark-client.spark-submit -v \
  --deploy-mode cluster \
  --username $1 \
  --namespace $2 \
  --conf spark.kubernetes.executor.request.cores=0.1 \
  --conf spark.kubernetes.executor.deleteOnTermination=false \
  --class org.apache.spark.examples.SparkPi \
  local:///opt/spark/examples/jars/spark-examples_2.12-3.5.8.jar \
  10000
