#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.


import logging
from pathlib import Path

import jubilant
import yaml

from .helpers import does_secret_exist, get_secret_data
from .types import AzureInfo, IntegrationTestsCharms

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]
CONTAINER_NAME = "test-container"
SECRET_NAME_PREFIX = "integrator-hub-conf-"


def test_build_and_deploy_hub_charm(juju: jubilant.Juju, deploy_hub_charm: str) -> None:
    juju.wait(lambda status: jubilant.all_active(status, APP_NAME))


def test_deploy_s3_integrator(
    juju: jubilant.Juju, charm_versions, deploy_s3_integrator_charm: str
) -> None:
    juju.wait(lambda status: jubilant.all_active(status, charm_versions.s3.application_name))


def test_deploy_azure_storage_integrator(
    juju: jubilant.Juju, charm_versions, azure_credentials: AzureInfo, platform: str
) -> None:
    juju.deploy(**charm_versions.azure_storage.deploy_dict(), constraints={"arch": platform})
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


def test_external_service_account_not_monitored(
    juju: jubilant.Juju, service_account: tuple[str, str]
) -> None:
    """Check that service accounts are not monitored by default.

    This test makes sure that we only inject spark properties into a secret *after* we configure the
    integration hub to monitor a specific service account.
    """
    name, namespace = service_account
    juju.wait(jubilant.all_active, delay=5)
    assert not does_secret_exist(namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{name}")

    juju.config(APP_NAME, {"monitored-service-accounts": f"{namespace}:{name}"})
    juju.wait(jubilant.all_active, delay=5)
    assert does_secret_exist(namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{name}")


def test_relation_with_s3(
    juju: jubilant.Juju, service_account: tuple[str, str], charm_versions: IntegrationTestsCharms
) -> None:
    service_account_name = service_account[0]
    namespace = service_account[1]
    logger.info(f"Service account: {service_account_name}, namespace: {namespace}")
    juju.config(APP_NAME, {"monitored-service-accounts": f"{namespace}:{service_account_name}"})
    juju.wait(jubilant.all_active, delay=5)

    # Verify that secret data is empty before S3 relation is added.
    secret_data = get_secret_data(
        namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )
    logger.info(f"namespace: {namespace} -> secret_data: {secret_data}")
    assert len(secret_data) == 0

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


def test_new_service_account_with_s3(
    juju: jubilant.Juju, service_account: tuple[str, str], charm_versions: IntegrationTestsCharms
) -> None:
    service_account_name = service_account[0]
    namespace = service_account[1]
    logger.info(f"Service account: {service_account_name}, namespace: {namespace}")

    juju.config(APP_NAME, {"monitored-service-accounts": f"{namespace}:{service_account_name}"})
    juju.wait(jubilant.all_active, delay=5)

    # check secret
    secret_data = get_secret_data(
        namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )
    assert len(secret_data) > 0
    assert "spark.hadoop.fs.s3a.access.key" in secret_data

    # Removing S3 <> Integration Hub relation
    juju.remove_relation(APP_NAME, charm_versions.s3.application_name)
    juju.wait(jubilant.all_active, delay=10)

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


def test_both_s3_and_azure_storage_integration(
    juju: jubilant.Juju, charm_versions: IntegrationTestsCharms
) -> None:
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
    juju: jubilant.Juju,
    service_account: tuple[str, str],
    charm_versions: IntegrationTestsCharms,
    azure_credentials: AzureInfo,
) -> None:
    service_account_name = service_account[0]
    namespace = service_account[1]
    logger.info(f"Service account: {service_account_name}, namespace: {namespace}")

    juju.config(APP_NAME, {"monitored-service-accounts": f"{namespace}:{service_account_name}"})
    juju.wait(jubilant.all_active, delay=5)
    # Verify that secret data is empty before S3 relation is added.
    secret_data = get_secret_data(
        namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )
    logger.info(f"namespace: {namespace} -> secret_data: {secret_data}")
    assert len(secret_data) == 0

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
    juju: jubilant.Juju,
    service_account: tuple[str, str],
    charm_versions: IntegrationTestsCharms,
    azure_credentials: AzureInfo,
) -> None:
    service_account_name = service_account[0]
    namespace = service_account[1]
    logger.info(f"Service account: {service_account_name}, namespace: {namespace}")

    juju.config(APP_NAME, {"monitored-service-accounts": f"{namespace}:{service_account_name}"})
    juju.wait(jubilant.all_active, delay=5)

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
    juju.wait(jubilant.all_active, delay=10)

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
    juju: jubilant.Juju,
    service_account: tuple[str, str],
    azure_credentials: AzureInfo,
) -> None:
    service_account_name = service_account[0]
    namespace = service_account[1]
    logger.info(f"Service account: {service_account_name}, namespace: {namespace}")

    juju.config(APP_NAME, {"monitored-service-accounts": f"{namespace}:{service_account_name}"})
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

    # Removing Spark Integration Hub application
    juju.remove_application(APP_NAME)
    juju.wait(jubilant.all_active, delay=5)

    secret_data = get_secret_data(
        namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )
    logger.info(f"secret data: {secret_data}")
    assert len(secret_data) == 0
