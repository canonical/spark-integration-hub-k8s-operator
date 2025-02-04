#!/bin/bash

spark-client.spark-submit -v --username $1 --namespace $2 --conf spark.kubernetes.executor.request.cores=0.1 --conf spark.kubernetes.container.image=ghcr.io/welpaolo/charmed-spark@sha256:acd322439b5e43dbf859e2c695c1fc9b133eb756332ad7026a80d90ff99e54b3 --class org.apache.spark.examples.SparkPi local:///opt/spark/examples/jars/spark-examples_2.12-3.4.2.jar 10000