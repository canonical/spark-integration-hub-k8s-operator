#!/bin/bash

sudo snap remove --purge spark-client
sudo snap install spark-client --channel=edge
mkdir -p ~/.kube
sudo microk8s config | tee ~/.kube/config

spark-client.service-account-registry delete --username $1 --namespace $2

spark-client.service-account-registry create --username $1 --namespace $2

spark-client.service-account-registry get-config --username $1 --namespace $2