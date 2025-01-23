#!/bin/bash

spark-client.spark-submit -v --username $1 --namespace $2 --conf spark.kubernetes.executor.request.cores=0.1 --conf spark.kubernetes.container.image=ghcr.io/welpaolo/charmed-spark@sha256:b24290cd49803af9b39d7022715835ca45cad3ced341f789e409fdf8ea331fa5 --class org.apache.spark.examples.SparkPi local:///opt/spark/examples/jars/spark-examples_2.12-3.4.2.jar 10000