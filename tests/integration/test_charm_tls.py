#!/usr/bin/env python3
# Copyright 2026 Canonical Limited
# See LICENSE file for licensing details.

import json
import logging
from pathlib import Path
from typing import cast

import jubilant
import yaml

from .helpers.integration_hub import (
    deploy_integration_hub_setup,
    deploy_test_charm_setup,
    get_integration_hub_secret_data,
    get_truststore_secret_data,
    integration_hub_secret_exists,
    prepare_s3_storage_setup,
)
from .helpers.spark import (
    assert_spark_job_successful,
    cleanup_workload_pods,
    run_spark_job,
    setup_spark_job,
)
from .types import IntegrationTestsCharms, S3Info

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]
TEST_CHARM_APP_NAME = "app"


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


def test_deploy_s3_integrator(
    juju: jubilant.Juju, charm_versions: IntegrationTestsCharms, s3_credentials: S3Info
) -> None:
    """Test deploying the S3 integrator charm."""
    prepare_s3_storage_setup(
        juju=juju,
        charm_versions=charm_versions,
        s3_credentials=s3_credentials,
        integrate=False,
        s3_tls=True,
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
    """Test the relation between the Spark Integration Hub and the S3 integrator."""
    service_account_name, namespace = service_account
    logger.info(f"Service account: {service_account_name}, namespace: {namespace}")
    juju.config(APP_NAME, {"monitored-service-accounts": f"{namespace}:{service_account_name}"})
    juju.wait(jubilant.all_active, delay=5)

    assert integration_hub_secret_exists(namespace, service_account_name)
    # Verify that secret data is empty before S3 relation is added.
    assert len(get_integration_hub_secret_data(namespace, service_account_name)) == 0

    # Relate S3 integrator with Spark Integration Hub
    juju.integrate(
        APP_NAME,
        charm_versions.s3.application_name,
    )
    juju.wait(jubilant.all_active, delay=5)

    secret_data = get_integration_hub_secret_data(namespace, service_account_name)
    assert len(secret_data) > 0
    assert "spark.hadoop.fs.s3a.access.key" in secret_data

    secret_data_truststore = get_truststore_secret_data(
        model_name=cast(str, juju.model), app_name=APP_NAME, namespace=namespace
    )
    assert len(secret_data_truststore) > 0

    setup_spark_job(namespace, service_account_name)
    run_spark_job(namespace, service_account_name)
    assert_spark_job_successful(namespace=namespace)
    cleanup_workload_pods(namespace)


def test_new_service_account_with_s3(
    juju: jubilant.Juju, service_account: tuple[str, str], charm_versions: IntegrationTestsCharms
) -> None:
    """Test that new service accounts also get the TLS configuration once S3 relation is added."""
    logger.info(
        "Testing that new service accounts also get the TLS configuration once S3 relation is added."
    )
    service_account_name, namespace = service_account
    logger.info(f"Service account: {service_account_name}, namespace: {namespace}")

    juju.config(APP_NAME, {"monitored-service-accounts": f"{namespace}:{service_account_name}"})
    juju.wait(jubilant.all_active, delay=10, timeout=120)

    # check secret
    secret_data = secret_data = get_integration_hub_secret_data(namespace, service_account_name)
    assert len(secret_data) > 0
    assert "spark.hadoop.fs.s3a.access.key" in secret_data

    secret_data_truststore = get_truststore_secret_data(
        model_name=cast(str, juju.model), app_name=APP_NAME, namespace=namespace
    )
    logger.info(f"namespace: {namespace} -> secret_data: {secret_data_truststore}")
    assert len(secret_data_truststore) > 0

    logger.info(
        "Remove integration with S3 to check that secrets are properly deleted and recreated..."
    )
    # Removing S3 <> Integration Hub relation
    juju.remove_relation(APP_NAME, charm_versions.s3.application_name)
    juju.wait(jubilant.all_active, delay=10, timeout=120)

    assert len(get_integration_hub_secret_data(namespace, service_account_name)) == 0
    secret_data_truststore = get_truststore_secret_data(
        model_name=cast(str, juju.model), app_name=APP_NAME, namespace=namespace
    )
    assert len(secret_data_truststore) == 0

    # Re-integrate S3 integrator with Spark Integration Hub
    juju.integrate(
        APP_NAME,
        charm_versions.s3.application_name,
    )
    juju.wait(jubilant.all_active, delay=5, timeout=120)

    secret_data = secret_data = get_integration_hub_secret_data(namespace, service_account_name)
    assert len(secret_data) > 0
    assert "spark.hadoop.fs.s3a.access.key" in secret_data

    secret_data_truststore = get_truststore_secret_data(
        model_name=cast(str, juju.model), app_name=APP_NAME, namespace=namespace
    )
    assert len(secret_data_truststore) > 0


def test_correct_tls_in_manifest(
    juju: jubilant.Juju,
    test_charm: Path,
    namespace: str,
) -> None:
    """Test that the TLS secret manifest is correctly generated and contains the expected content."""
    deploy_test_charm_setup(juju=juju, test_charm=test_charm, spark_workload_namespace=namespace)

    logger.info("Enable autoscaling...")
    juju.config(APP_NAME, {"enable-dynamic-allocation": "true"})
    juju.wait(
        lambda status: jubilant.all_active(status) and jubilant.all_agents_idle(status), delay=5
    )

    # The added spark properties must be reflected on the requirer charm
    task = juju.run(f"{TEST_CHARM_APP_NAME}/0", "get-properties-sa1")
    assert task.return_code == 0
    assert "spark-properties" in task.results
    properties = task.results["spark-properties"]
    assert "spark.dynamicAllocation.enabled" in json.loads(properties)

    task = juju.run(f"{TEST_CHARM_APP_NAME}/0", "get-resource-manifest-sa1")
    assert task.return_code == 0
    assert "resource-manifest" in task.results
    manifest = task.results["resource-manifest"]
    assert manifest is not None
    assert manifest.strip() != ""
    logger.info(f"Generated manifest:\n{manifest}")

    assert "truststore.jks" in manifest


def test_remove_application(
    juju: jubilant.Juju,
    service_account: tuple[str, str],
) -> None:
    """Test removing the Spark Integration Hub application and verifying that the associated secrets are deleted."""
    service_account_name, namespace = service_account
    logger.info(f"Service account: {service_account_name}, namespace: {namespace}")

    juju.config(APP_NAME, {"monitored-service-accounts": f"{namespace}:{service_account_name}"})
    juju.wait(jubilant.all_active, delay=5)

    secret_data = secret_data = get_integration_hub_secret_data(namespace, service_account_name)
    assert len(secret_data) > 0
    assert "spark.hadoop.fs.s3a.access.key" in secret_data

    # Removing Spark Integration Hub application
    juju.remove_application(APP_NAME)
    juju.wait(jubilant.all_active, delay=5)

    assert len(get_integration_hub_secret_data(namespace, service_account_name)) == 0
    secret_data_truststore = get_truststore_secret_data(
        model_name=cast(str, juju.model), app_name=APP_NAME, namespace=namespace
    )
    assert len(secret_data_truststore) == 0
