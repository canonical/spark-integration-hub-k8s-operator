#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

import base64
import json
import logging
import time
from pathlib import Path

import jubilant
import pytest
import yaml

from .helpers import (
    get_secret_data,
)

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]
CONTAINER_NAME = "test-container"
SECRET_NAME_PREFIX = "integrator-hub-conf-"


def test_build_and_deploy_hub_charm(juju: jubilant.Juju, deploy_hub_charm) -> None:
    juju.wait(lambda status: jubilant.all_active(status, APP_NAME))


def test_deploy_s3_integrator(
    juju: jubilant.Juju, charm_versions, deploy_s3_integrator_charm
) -> None:
    juju.wait(lambda status: jubilant.all_active(status, charm_versions.s3.application_name))


def test_deploy_azure_storage_integrator(
    juju: jubilant.Juju, charm_versions, azure_credentials: dict[str, str]
) -> None:
    juju.deploy(**charm_versions.azure_storage.deploy_dict())
    juju.wait(jubilant.all_agents_idle)

    secret_uri = juju.add_secret(
        "azure-credentials",
        content={
            "secret-key": azure_credentials["secret-key"],
        },
    )
    juju.cli("grant-secret", secret_uri, charm_versions.azure_storage.application_name)
    juju.config(
        charm_versions.azure_storage.application_name,
        {
            "container": azure_credentials["container"],
            "path": azure_credentials["path"],
            "storage-account": azure_credentials["storage-account"],
            "connection-protocol": azure_credentials["connection-protocol"],
            "credentials": secret_uri,
        },
    )
    # juju.wait(lambda status: jubilant.all_active(status, charm_versions.azure_storage.application_name))
    juju.wait(jubilant.all_active)


@pytest.mark.parametrize(
    "conf_key,conf_value", [("a", "b"), ("foo.bar.grok", "val"), ("key", "iam=secret==val")]
)
def test_integration_hub_actions(
    juju: jubilant.Juju, service_account, conf_key, conf_value
) -> None:
    """Test integration hub actions like add-config, list-config, remove-config, etc."""
    service_account_name = service_account[0]
    namespace = service_account[1]
    logger.info(f"Service account: {service_account_name}, namespace: {namespace}")

    # Wait for some time for the secrets to be reflected
    logger.info("Waiting for 10 seconds...")
    time.sleep(10)

    # Verify that secret data is empty before any configurations are added.
    secret_data = get_secret_data(
        namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )
    logger.info(f"namespace: {namespace} -> secret_data: {secret_data}")
    assert len(secret_data) == 0
    task = juju.run(
        f"{APP_NAME}/0",
        "list-config",
    )
    assert task.return_code == 0
    props = json.loads(task.results["properties"])
    assert len(props) == 0

    # Add new configuration
    task = juju.run(f"{APP_NAME}/0", "add-config", params={"conf": f"{conf_key}={conf_value}"})
    assert task.return_code == 0

    # Wait for some time for the secrets to be reflected
    logger.info("Waiting for 10 seconds...")
    time.sleep(10)

    secret_data = get_secret_data(
        namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )
    logger.info(f"namespace: {namespace} -> secret_data: {secret_data}")
    assert len(secret_data) > 0
    assert conf_key in secret_data
    assert base64.b64decode(secret_data[conf_key]).decode() == conf_value

    task = juju.run(
        f"{APP_NAME}/0",
        "list-config",
    )
    assert task.return_code == 0
    props = json.loads(task.results["properties"])
    logger.info(f"Results from list-config action: {props}")
    assert len(props) > 0
    assert props[conf_key] == conf_value

    # Remove the added config
    task = juju.run(f"{APP_NAME}/0", "remove-config", params={"key": conf_key})
    assert task.return_code == 0

    # Wait for some time for the changes in secrets to be reflected
    logger.info("Waiting for 10 seconds...")
    time.sleep(10)

    secret_data = get_secret_data(
        namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )
    logger.info(f"namespace: {namespace} -> secret_data: {secret_data}")
    assert len(secret_data) == 0

    task = juju.run(
        f"{APP_NAME}/0",
        "list-config",
    )
    assert task.return_code == 0
    props = json.loads(task.results["properties"])
    logger.info(f"Results from list-config action: {props}")
    assert len(props) == 0


def test_relation_with_s3(juju: jubilant.Juju, service_account, charm_versions) -> None:
    service_account_name = service_account[0]
    namespace = service_account[1]
    logger.info(f"Service account: {service_account_name}, namespace: {namespace}")

    # Verify that secret data is empty before S3 relation is added.
    secret_data = get_secret_data(
        namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )
    logger.info(f"namespace: {namespace} -> secret_data: {secret_data}")
    assert len(secret_data) == 0
    task = juju.run(
        f"{APP_NAME}/0",
        "list-config",
    )
    assert task.return_code == 0
    props = json.loads(task.results["properties"])
    assert len(props) == 0

    # Relate S3 integrator with Spark Integration Hub
    juju.integrate(
        APP_NAME,
        charm_versions.s3.application_name,
    )
    juju.wait(jubilant.all_active, delay=5)

    secret_data = get_secret_data(
        namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )
    logger.info(f"namespace: {namespace} -> secret_data: {secret_data}")
    assert len(secret_data) > 0
    assert "spark.hadoop.fs.s3a.access.key" in secret_data


def test_new_service_account_with_s3(juju: jubilant.Juju, service_account, charm_versions) -> None:
    service_account_name = service_account[0]
    namespace = service_account[1]
    logger.info(f"Service account: {service_account_name}, namespace: {namespace}")

    # Wait for some time for the secrets to be reflected
    logger.info("Waiting for 10 seconds...")
    time.sleep(10)

    # check secret
    secret_data = get_secret_data(
        namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )
    assert len(secret_data) > 0
    assert "spark.hadoop.fs.s3a.access.key" in secret_data

    # Removing S3 <> Integration Hub relation
    juju.remove_relation(APP_NAME, charm_versions.s3.application_name)
    juju.wait(jubilant.all_active)

    secret_data = get_secret_data(
        namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )
    assert len(secret_data) == 0

    # Re-integrate S3 integrator with Spark Integration Hub
    juju.integrate(
        APP_NAME,
        charm_versions.s3.application_name,
    )
    juju.wait(jubilant.all_active, delay=5)

    secret_data = get_secret_data(
        namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )
    logger.info(f"namespace: {namespace} -> secret_data: {secret_data}")
    assert len(secret_data) > 0
    assert "spark.hadoop.fs.s3a.access.key" in secret_data


def test_both_s3_and_azure_storage_integration(juju: jubilant.Juju, charm_versions) -> None:
    logger.info(
        "Relating spark integration hub charm with azure-storage-integrator along with existing relation with s3-integrator charm"
    )
    juju.integrate(APP_NAME, charm_versions.azure_storage.application_name)
    juju.wait(jubilant.all_agents_idle)
    juju.wait(lambda status: jubilant.all_blocked(status, APP_NAME))

    # Now remove relation with both S3 and Azure Storage

    juju.remove_relation(APP_NAME, charm_versions.azure_storage.application_name)
    juju.remove_relation(APP_NAME, charm_versions.s3.application_name)
    juju.wait(jubilant.all_active)


def test_relation_with_azure_storage(
    juju: jubilant.Juju, service_account, charm_versions, azure_credentials
) -> None:
    service_account_name = service_account[0]
    namespace = service_account[1]
    logger.info(f"Service account: {service_account_name}, namespace: {namespace}")

    # Verify that secret data is empty before S3 relation is added.
    secret_data = get_secret_data(
        namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )
    logger.info(f"namespace: {namespace} -> secret_data: {secret_data}")
    assert len(secret_data) == 0
    task = juju.run(
        f"{APP_NAME}/0",
        "list-config",
    )
    assert task.return_code == 0
    props = json.loads(task.results["properties"])
    assert len(props) == 0

    # Relate Azure Storage integrator with Spark Integration Hub
    juju.integrate(
        APP_NAME,
        charm_versions.azure_storage.application_name,
    )
    juju.wait(jubilant.all_active, delay=5)

    secret_data = get_secret_data(
        namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )
    logger.info(f"namespace: {namespace} -> secret_data: {secret_data}")
    assert len(secret_data) > 0
    assert (
        f"spark.hadoop.fs.azure.account.key.{azure_credentials['storage-account']}.dfs.core.windows.net"
        in secret_data
    )


def test_new_service_account_with_azure_storage(
    juju: jubilant.Juju, service_account, charm_versions, azure_credentials
) -> None:
    service_account_name = service_account[0]
    namespace = service_account[1]
    logger.info(f"Service account: {service_account_name}, namespace: {namespace}")

    # Wait for some time for the secrets to be reflected
    logger.info("Waiting for 10 seconds...")
    time.sleep(10)

    # check secret
    secret_data = get_secret_data(
        namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )
    assert len(secret_data) > 0
    assert (
        f"spark.hadoop.fs.azure.account.key.{azure_credentials['storage-account']}.dfs.core.windows.net"
        in secret_data
    )

    # Removing Azure Storage <> Integration Hub relation
    juju.remove_relation(APP_NAME, charm_versions.azure_storage.application_name)
    juju.wait(jubilant.all_active)

    secret_data = get_secret_data(
        namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )
    assert len(secret_data) == 0

    # Re-integrate Azure Storage integrator with Spark Integration Hub
    juju.integrate(
        APP_NAME,
        charm_versions.azure_storage.application_name,
    )
    juju.wait(jubilant.all_active, delay=5)

    secret_data = get_secret_data(
        namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )
    logger.info(f"namespace: {namespace} -> secret_data: {secret_data}")
    assert len(secret_data) > 0
    assert (
        f"spark.hadoop.fs.azure.account.key.{azure_credentials['storage-account']}.dfs.core.windows.net"
        in secret_data
    )


def test_remove_application(
    juju: jubilant.Juju, service_account, charm_versions, azure_credentials
) -> None:
    service_account_name = service_account[0]
    namespace = service_account[1]
    logger.info(f"Service account: {service_account_name}, namespace: {namespace}")

    # Wait for some time for the changes in secrets to be reflected
    logger.info("Waiting for 10 seconds...")
    time.sleep(10)

    secret_data = get_secret_data(
        namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )
    logger.info(f"namespace: {namespace} -> secret_data: {secret_data}")
    assert len(secret_data) > 0
    assert (
        f"spark.hadoop.fs.azure.account.key.{azure_credentials['storage-account']}.dfs.core.windows.net"
        in secret_data
    )

    # Removing Spark Integration Hub application
    juju.remove_application(APP_NAME)
    juju.wait(jubilant.all_active)

    secret_data = get_secret_data(
        namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )
    logger.info(f"secret data: {secret_data}")
    assert len(secret_data) == 0
