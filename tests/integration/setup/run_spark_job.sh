#!/bin/bash

spark-client.spark-submit -v --username $1 --namespace $2 --conf spark.kubernetes.executor.request.cores=0.1 --class org.apache.spark.examples.SparkPi local:///opt/spark/examples/jars/spark-examples_2.12-3.4.2.jar 10000