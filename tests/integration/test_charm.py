#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.


import logging
from pathlib import Path
from typing import cast

import jubilant
import lightkube
import pytest
import yaml

from .helpers.azure_storage import prepare_azure_storage_setup
from .helpers.integration_hub import (
    deploy_integration_hub_setup,
    get_integration_hub_secret_data,
    integration_hub_secret_exists,
)
from .helpers.juju import get_unit_pod_names
from .helpers.k8s import assert_security_context, generate_container_securitycontext_map
from .helpers.s3 import prepare_s3_storage_setup
from .types import AzureInfo, IntegrationTestsCharms, S3Info

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]
CONTAINER_NAME = "test-container"
SECRET_NAME_PREFIX = "integrator-hub-conf-"
CONTAINERS_SECURITY_CONTEXT_MAP = generate_container_securitycontext_map(METADATA)


def test_deploy_integration_hub(
    juju: jubilant.Juju, charm_versions: IntegrationTestsCharms, hub_charm: str | Path
) -> None:
    """Test deploying the integration hub charm."""
    deploy_integration_hub_setup(
        juju=juju,
        hub_charm=hub_charm,
        charm_versions=charm_versions,
        trust=True,
    )
    juju.wait(jubilant.all_active)


@pytest.mark.parametrize("container_name", list(CONTAINERS_SECURITY_CONTEXT_MAP.keys()))
def test_container_security_context(
    juju: jubilant.Juju,
    container_name: str,
) -> None:
    """Test container security context is correctly set.

    Verify that container spec defines the security context with correct
    user ID and group ID.
    """
    lightkube_client = lightkube.Client()
    pod_name = get_unit_pod_names(cast(str, juju.model), APP_NAME)[0]
    assert_security_context(
        lightkube_client,
        pod_name,
        container_name,
        CONTAINERS_SECURITY_CONTEXT_MAP,
        cast(str, juju.model),
    )


def test_deploy_s3_integrator(
    juju: jubilant.Juju, charm_versions: IntegrationTestsCharms, s3_credentials: S3Info
) -> None:
    prepare_s3_storage_setup(
        juju=juju, charm_versions=charm_versions, s3_credentials=s3_credentials, integrate=False
    )
    juju.wait(jubilant.all_active)


def test_deploy_azure_storage_integrator(
    juju: jubilant.Juju, charm_versions: IntegrationTestsCharms, azure_credentials: AzureInfo
) -> None:
    prepare_azure_storage_setup(
        juju=juju,
        charm_versions=charm_versions,
        azure_storage_credentials=azure_credentials,
        integrate=False,
    )
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
    assert not integration_hub_secret_exists(namespace, name)

    juju.config(APP_NAME, {"monitored-service-accounts": f"{namespace}:{name}"})
    juju.wait(jubilant.all_active, delay=5)
    assert integration_hub_secret_exists(namespace, name)


def test_relation_with_s3(
    juju: jubilant.Juju, service_account: tuple[str, str], charm_versions: IntegrationTestsCharms
) -> None:
    service_account_name, namespace = service_account
    logger.info(f"Service account: {service_account_name}, namespace: {namespace}")
    juju.config(APP_NAME, {"monitored-service-accounts": f"{namespace}:{service_account_name}"})
    juju.wait(jubilant.all_active, delay=5)

    assert integration_hub_secret_exists(namespace, service_account_name)
    # Verify that secret data is empty before S3 relation is added.
    assert len(get_integration_hub_secret_data(namespace, service_account_name)) == 0

    logger.info("Integrating S3 integrator with Spark Integration Hub...")
    juju.integrate(
        APP_NAME,
        charm_versions.s3.application_name,
    )
    juju.wait(jubilant.all_active, delay=5)

    assert integration_hub_secret_exists(namespace, service_account_name)
    secret_data = get_integration_hub_secret_data(namespace, service_account_name)
    assert len(secret_data) > 0
    assert "spark.hadoop.fs.s3a.access.key" in secret_data


def test_new_service_account_with_s3(
    juju: jubilant.Juju, service_account: tuple[str, str], charm_versions: IntegrationTestsCharms
) -> None:
    service_account_name, namespace = service_account
    logger.info(f"Service account: {service_account_name}, namespace: {namespace}")

    juju.config(APP_NAME, {"monitored-service-accounts": f"{namespace}:{service_account_name}"})
    juju.wait(jubilant.all_active, delay=5)

    # check secret
    secret_data = get_integration_hub_secret_data(namespace, service_account_name)
    assert len(secret_data) > 0
    assert "spark.hadoop.fs.s3a.access.key" in secret_data

    # Removing S3 <> Integration Hub relation
    juju.remove_relation(APP_NAME, charm_versions.s3.application_name)
    juju.wait(jubilant.all_active, delay=10)

    secret_data = get_integration_hub_secret_data(namespace, service_account_name)
    assert len(secret_data) == 0

    # Re-integrate S3 integrator with Spark Integration Hub
    juju.integrate(
        APP_NAME,
        charm_versions.s3.application_name,
    )
    juju.wait(jubilant.all_active, delay=5)

    secret_data = get_integration_hub_secret_data(namespace, service_account_name)
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
    service_account_name, namespace = service_account
    logger.info(f"Service account: {service_account_name}, namespace: {namespace}")

    juju.config(APP_NAME, {"monitored-service-accounts": f"{namespace}:{service_account_name}"})
    juju.wait(jubilant.all_active, delay=5)
    # Verify that secret data is empty before Azure storage relation is added.
    secret_data = get_integration_hub_secret_data(namespace, service_account_name)
    assert len(secret_data) == 0

    # Relate Azure Storage integrator with Spark Integration Hub
    juju.integrate(
        APP_NAME,
        charm_versions.azure_storage.application_name,
    )
    juju.wait(jubilant.all_active, delay=5)

    secret_data = get_integration_hub_secret_data(namespace, service_account_name)
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
    service_account_name, namespace = service_account
    logger.info(f"Service account: {service_account_name}, namespace: {namespace}")

    juju.config(APP_NAME, {"monitored-service-accounts": f"{namespace}:{service_account_name}"})
    juju.wait(jubilant.all_active, delay=5)

    # check secret
    secret_data = get_integration_hub_secret_data(namespace, service_account_name)
    assert len(secret_data) > 0
    assert (
        f"spark.hadoop.fs.azure.account.key.{azure_credentials['storage-account']}.dfs.core.windows.net"
        in secret_data
    )

    # Removing Azure Storage <> Integration Hub relation
    juju.remove_relation(APP_NAME, charm_versions.azure_storage.application_name)
    juju.wait(jubilant.all_active, delay=10)

    secret_data = get_integration_hub_secret_data(namespace, service_account_name)
    assert len(secret_data) == 0

    # Re-integrate Azure Storage integrator with Spark Integration Hub
    juju.integrate(
        APP_NAME,
        charm_versions.azure_storage.application_name,
    )
    juju.wait(jubilant.all_active, delay=5)

    secret_data = get_integration_hub_secret_data(namespace, service_account_name)
    assert len(secret_data) > 0
    assert (
        f"spark.hadoop.fs.azure.account.key.{azure_credentials['storage-account']}.dfs.core.windows.net"
        in secret_data
    )


def test_multiple_units_status(
    juju: jubilant.Juju,
) -> None:
    logger.info("Testing that deploying multiple units sets the appropriate status")
    juju.wait(lambda status: jubilant.all_active(status, APP_NAME), delay=5)
    juju.add_unit(APP_NAME)
    juju.wait(lambda status: jubilant.all_blocked(status, APP_NAME), delay=5)
    logger.info("Remove unit and check for active status again.")
    juju.remove_unit(APP_NAME, num_units=1)
    juju.wait(lambda status: jubilant.all_active(status, APP_NAME), delay=5)


def test_remove_application(
    juju: jubilant.Juju,
    service_account: tuple[str, str],
    azure_credentials: AzureInfo,
) -> None:
    service_account_name, namespace = service_account
    logger.info(f"Service account: {service_account_name}, namespace: {namespace}")

    juju.config(APP_NAME, {"monitored-service-accounts": f"{namespace}:{service_account_name}"})
    juju.wait(jubilant.all_active, delay=5)

    secret_data = get_integration_hub_secret_data(namespace, service_account_name)
    assert len(secret_data) > 0
    assert (
        f"spark.hadoop.fs.azure.account.key.{azure_credentials['storage-account']}.dfs.core.windows.net"
        in secret_data
    )

    # Removing Spark Integration Hub application
    juju.remove_application(APP_NAME)
    juju.wait(jubilant.all_active, delay=5)

    secret_data = get_integration_hub_secret_data(namespace, service_account_name)
    logger.info(f"secret data: {secret_data}")
    assert len(secret_data) == 0
