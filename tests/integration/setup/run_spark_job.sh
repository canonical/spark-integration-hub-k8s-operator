#!/bin/bash

spark-client.spark-submit -v --username $1 \
--namespace $2 \
--conf spark.kubernetes.executor.request.cores=0.1 \
--conf spark.kubernetes.container.image=ghcr.io/canonical/charmed-spark@sha256:5ee407585ff35d04cc6ec82a87150e43ccbaec337de06c9a2b12cd95798031ab \
--class org.apache.spark.examples.SparkPi local:///opt/spark/examples/jars/spark-examples_2.13-4.0.1.jar 10000
