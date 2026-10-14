#!/bin/bash

spark-client.service-account-registry delete --username $1 --namespace $2

spark-client.service-account-registry create --username $1 --namespace $2

spark-client.service-account-registry get-config --username $1 --namespace $2
