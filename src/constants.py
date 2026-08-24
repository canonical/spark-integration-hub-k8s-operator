#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.
#
# Learn more at: https://juju.is/docs/sdk

"""Literals and constants."""

CONTAINER = "integration-hub"
INTEGRATION_HUB_LABEL = "app.kubernetes.io/managed-by=integration-hub"

PEBBLE_USER = ("_daemon_", "_daemon_")

# integrations
INTEGRATION_HUB_REL = "spark-service-account"
S3_RELATION_NAME = "s3-credentials"
AZURE_RELATION_NAME = "azure-storage-credentials"
PUSHGATEWAY = "cos"
LOGGING_RELATION_NAME = "logging"
PEER_REL = "hub-peers"

TRUSTSTORE_PASSWORD_KEY = "truststore_password"
TRUSTSTORE_PATH_KEY = "truststore_path"
TRUSTSTORE_SECRET_NAME_KEY = "truststore_secret_name"

# Base directory where the S3 truststore secret is mounted in the Spark
# driver/executor pods. It must be writable by the `_daemon_` (584792) user so
# that non-root workloads (e.g. Kyuubi) can replicate the file for client-side
# operations.
TRUSTSTORE_MOUNT_BASE = "/etc/spark8t/conf"
