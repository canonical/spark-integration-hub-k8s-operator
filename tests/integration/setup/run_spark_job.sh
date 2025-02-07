#!/bin/bash

spark-client.spark-submit -v --username $1 --namespace $2 --conf spark.kubernetes.executor.request.cores=0.1 --conf spark.kubernetes.container.image=ghcr.io/canonical/charmed-spark@sha256:cc8635070e8fecfb5d09201225e1510280149960b57a192959f7ad3d18840503 --class org.apache.spark.examples.SparkPi local:///opt/spark/examples/jars/spark-examples_2.12-3.4.2.jar 10000