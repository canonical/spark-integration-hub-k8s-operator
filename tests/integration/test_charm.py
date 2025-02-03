#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.


import asyncio
import base64
import json
import logging
import subprocess
from pathlib import Path

import pytest
import yaml
from pytest_operator.plugin import OpsTest

from .helpers import (
    BUCKET_NAME,
    add_juju_secret,
    fetch_action_sync_s3_credentials,
    flatten,
    get_secret_data,
    juju_sleep,
    run_action,
    setup_s3_bucket_for_sch_server,
)

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]
# BUCKET_NAME = "test-bucket"
CONTAINER_NAME = "test-container"
SECRET_NAME_PREFIX = "integrator-hub-conf-"


@pytest.mark.abort_on_fail
async def test_build_and_deploy(ops_test: OpsTest, charm_versions, azure_credentials):
    """Build the charm-under-test and deploy it together with related charms.

    Assert on the unit status before any relations/configurations take place.
    """
    logger.info("Setting up minio.....")

    setup_minio_output = (
        subprocess.check_output(
            "./tests/integration/setup/setup_minio.sh | tail -n 1", shell=True, stderr=None
        )
        .decode("utf-8")
        .strip()
    )

    logger.info(f"Minio output:\n{setup_minio_output}")

    s3_params = setup_minio_output.strip().split(",")
    endpoint_url = s3_params[0]
    access_key = s3_params[1]
    secret_key = s3_params[2]

    logger.info(
        f"Setting up s3 bucket with endpoint_url={endpoint_url}, access_key={access_key}, secret_key={secret_key}"
    )

    setup_s3_bucket_for_sch_server(endpoint_url, access_key, secret_key)

    logger.info("Bucket setup complete")

    logger.info("Building charm")
    # Build and deploy charm from local source folder

    charm = await ops_test.build_charm(".")

    image_version = METADATA["resources"]["integration-hub-image"]["upstream-source"]

    logger.info(f"Image version: {image_version}")

    resources = {"integration-hub-image": image_version}

    logger.info(
        "Deploying Spark Integration hub charm, s3-integrator charm and azure-storage-integrator charm"
    )

    # Deploy the charm and wait for waiting status
    await asyncio.gather(
        ops_test.model.deploy(**charm_versions.s3.deploy_dict()),
        ops_test.model.deploy(**charm_versions.azure_storage.deploy_dict()),
        ops_test.model.deploy(
            charm,
            resources=resources,
            application_name=APP_NAME,
            num_units=1,
            series="jammy",
            trust=True,
        ),
    )

    logger.info("Waiting for s3-integrator and azure-storage-integrator charms to be idle...")
    await ops_test.model.wait_for_idle(
        apps=[
            APP_NAME,
            charm_versions.s3.application_name,
            charm_versions.azure_storage.application_name,
        ],
        timeout=300,
    )

    s3_integrator_unit = ops_test.model.applications[charm_versions.s3.application_name].units[0]

    logger.info("Setting up s3 credentials in s3-integrator charm")

    await fetch_action_sync_s3_credentials(
        s3_integrator_unit, access_key=access_key, secret_key=secret_key
    )
    async with ops_test.fast_forward():
        await ops_test.model.wait_for_idle(
            apps=[charm_versions.s3.application_name], status="active"
        )

    configuration_parameters = {
        "bucket": BUCKET_NAME,
        "path": "spark-events",
        "endpoint": endpoint_url,
    }
    # apply new configuration options
    logger.info("Setting up configuration for s3-integrator charm...")
    await ops_test.model.applications[charm_versions.s3.application_name].set_config(
        configuration_parameters
    )

    logger.info("Adding Juju secret for secret-key config option for azure-storage-integrator")
    credentials_secret_uri = await add_juju_secret(
        ops_test,
        charm_versions.azure_storage.application_name,
        "iamsecret",
        {"secret-key": azure_credentials["secret-key"]},
    )
    logger.info(
        f"Juju secret for secret-key config option for azure-storage-integrator added. Secret URI: {credentials_secret_uri}"
    )

    configuration_parameters = {
        "container": azure_credentials["container"],
        "path": azure_credentials["path"],
        "storage-account": azure_credentials["storage-account"],
        "connection-protocol": azure_credentials["connection-protocol"],
        "credentials": credentials_secret_uri,
    }
    # apply new configuration options
    logger.info("Setting up configuration for azure-storage-integrator charm...")
    await ops_test.model.applications[charm_versions.azure_storage.application_name].set_config(
        configuration_parameters
    )

    logger.info(
        "Waiting for s3-integrator, azure-storage-integrator and integration-hub charm to be idle and active..."
    )
    async with ops_test.fast_forward():
        await ops_test.model.wait_for_idle(
            apps=[
                charm_versions.azure_storage.application_name,
                charm_versions.s3.application_name,
                APP_NAME,
            ],
            status="active",
        )

    logger.info("Deploying the grafana-agent-k8s charm")
    await ops_test.model.deploy(**charm_versions.grafana_agent.deploy_dict())
    logger.debug(
        "Waiting for %s to by in blocked state", charm_versions.grafana_agent.application_name
    )
    # Note(rgildein): The grafana-agent-k8s charm is in blocked state, since we are not deploying whole cos.
    await ops_test.model.wait_for_idle(
        apps=[charm_versions.grafana_agent.application_name], status="blocked"
    )


@pytest.mark.abort_on_fail
@pytest.mark.parametrize("conf_key,conf_value", [("a", "b"), ("foo.bar.grok", "val")])
async def test_actions(ops_test: OpsTest, namespace, service_account, conf_key, conf_value):
    logger.info("Testing actions")
    service_account_name = service_account[0]
    logger.info(f"Service account name: {service_account_name}")
    secret_data = get_secret_data(
        namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )
    logger.info(f"namespace: {namespace} -> secret_data: {secret_data}")
    assert len(secret_data) == 0

    # list config
    res = await run_action(ops_test, "list-config", {}, APP_NAME)
    assert res["return-code"] == 0
    # wait for active status
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME],
        status="active",
        timeout=1000,
    )
    logger.info(f"List config action result: {res}")
    secret_data = get_secret_data(
        namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )
    logger.info(f"namespace: {namespace} -> secret_data: {secret_data}")
    assert len(secret_data) == 0

    # add new configuration
    res = await run_action(ops_test, "add-config", {"conf": f"{conf_key}={conf_value}"}, APP_NAME)
    assert res["return-code"] == 0
    # wait for active status
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME],
        status="active",
        timeout=1000,
    )
    logger.info(f"add-config action result: {res}")

    await juju_sleep(ops_test, APP_NAME, 15)

    secret_data = get_secret_data(
        namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )
    logger.info(f"namespace: {namespace} -> secret_data: {secret_data}")
    # check data in secret
    assert conf_key in secret_data
    assert len(secret_data) > 0

    # check that previously set configuration option is present
    res = await run_action(ops_test, "list-config", {}, APP_NAME)
    assert res["return-code"] == 0
    # wait for active status
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME],
        status="active",
        timeout=1000,
    )
    logger.info(f"List-config action result: {res}")
    secret_data = get_secret_data(
        namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )
    logger.info(f"namespace: {namespace} -> secret_data: {secret_data}")
    assert len(secret_data) > 0
    assert conf_key in flatten(json.loads(res.get("properties", {})))

    # Remove inserted config
    res = await run_action(ops_test, "remove-config", {"key": conf_key}, APP_NAME)
    assert res["return-code"] == 0
    # wait for active status
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME],
        status="active",
        timeout=1000,
    )
    logger.info(f"Remove-config action result: {res}")

    await juju_sleep(ops_test, APP_NAME, 15)

    secret_data = get_secret_data(
        namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )
    logger.info(f"namespace: {namespace} -> secret_data: {secret_data}")
    assert len(secret_data) == 0


@pytest.mark.abort_on_fail
async def test_add_config_with_equal_sign(ops_test: OpsTest, namespace, service_account):
    service_account_name = service_account[0]

    # add new configuration whose value contains '=' characters
    res = await run_action(ops_test, "add-config", {"conf": "key=iam=secret=="}, APP_NAME)
    assert res["return-code"] == 0
    # wait for active status
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME],
        status="active",
        timeout=1000,
    )
    logger.info(f"add-config action result: {res}")

    await juju_sleep(ops_test, APP_NAME, 15)

    secret_data = get_secret_data(
        namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )
    logger.info(f"namespace: {namespace} -> secret_data: {secret_data}")
    # check data in secret
    assert "key" in secret_data
    assert len(secret_data) > 0
    value = base64.b64decode(secret_data["key"]).decode()
    assert value == "iam=secret=="


@pytest.mark.abort_on_fail
async def test_add_new_service_account_with_config_value_containing_equals_sign(
    ops_test: OpsTest, namespace, service_account
):
    service_account_name = service_account[0]

    # wait for the update of secrets
    await juju_sleep(ops_test, APP_NAME, 15)

    # check secret
    secret_data = get_secret_data(
        namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )

    # check data in secret
    assert "key" in secret_data
    assert len(secret_data) > 0
    value = base64.b64decode(secret_data["key"]).decode()
    assert value == "iam=secret=="

    # clear config
    res = await run_action(ops_test, "clear-config", {}, APP_NAME)
    assert res["return-code"] == 0
    # wait for active status
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME],
        status="active",
        timeout=1000,
    )

    await juju_sleep(ops_test, APP_NAME, 15)

    secret_data = get_secret_data(
        namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )
    logger.info(f"namespace: {namespace} -> secret_data: {secret_data}")
    logger.info(f"Clear-config action result: {res}")
    assert len(secret_data) == 0


@pytest.mark.abort_on_fail
async def test_relation_to_s3(ops_test: OpsTest, charm_versions, namespace, service_account):
    logger.info("Relating spark integration hub charm with s3-integrator charm")
    service_account_name = service_account[0]
    secret_data = get_secret_data(
        namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )
    logger.info(f"namespace: {namespace} -> secret_data: {secret_data}")
    assert len(secret_data) == 0

    await ops_test.model.add_relation(charm_versions.s3.application_name, APP_NAME)

    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, charm_versions.s3.application_name], timeout=1000
    )

    # wait for active status
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME],
        status="active",
        timeout=1000,
    )

    # wait for secret update
    logger.info("Wait for secret update.")

    await juju_sleep(ops_test, APP_NAME, 15)

    secret_data = get_secret_data(
        namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )
    logger.info(f"namespace: {namespace} -> secret_data: {secret_data}")
    assert len(secret_data) > 0
    assert "spark.hadoop.fs.s3a.access.key" in secret_data


@pytest.mark.abort_on_fail
async def test_add_new_service_account_with_s3(ops_test: OpsTest, namespace, service_account):
    service_account_name = service_account[0]

    # wait for the update of secrets
    await juju_sleep(ops_test, APP_NAME, 15)

    # check secret
    secret_data = get_secret_data(
        namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )
    assert len(secret_data) > 0
    assert "spark.hadoop.fs.s3a.access.key" in secret_data


@pytest.mark.abort_on_fail
async def test_add_removal_s3_relation(
    ops_test: OpsTest, namespace, service_account, charm_versions
):
    service_account_name = service_account[0]

    # wait for the update of secrets
    await juju_sleep(ops_test, APP_NAME, 15)

    # check secret
    secret_data = get_secret_data(
        namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )

    assert len(secret_data) > 0
    assert "spark.hadoop.fs.s3a.access.key" in secret_data

    await ops_test.model.applications[APP_NAME].remove_relation(
        f"{APP_NAME}:s3-credentials", f"{charm_versions.s3.application_name}:s3-credentials"
    )

    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, charm_versions.s3.application_name],
        status="active",
        timeout=300,
        idle_period=30,
    )

    secret_data = get_secret_data(
        namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )
    assert len(secret_data) == 0

    await ops_test.model.add_relation(charm_versions.s3.application_name, APP_NAME)

    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, charm_versions.s3.application_name], timeout=1000
    )

    # wait for active status
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME],
        status="active",
        timeout=1000,
    )

    # wait for secret update
    logger.info("Wait for secret update.")
    await juju_sleep(ops_test, APP_NAME, 15)

    secret_data = get_secret_data(
        namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )
    logger.info(f"namespace: {namespace} -> secret_data: {secret_data}")
    assert len(secret_data) > 0
    assert "spark.hadoop.fs.s3a.access.key" in secret_data


@pytest.mark.abort_on_fail
async def test_relation_to_both_s3_and_azure_storage_at_same_time(
    ops_test: OpsTest, charm_versions
):
    logger.info(
        "Relating spark integration hub charm with azure-storage-integrator along with existing relation with s3-integrator charm"
    )

    await ops_test.model.add_relation(charm_versions.azure_storage.application_name, APP_NAME)

    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, charm_versions.azure_storage.application_name], timeout=1000
    )

    # wait for blocked status
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME],
        status="blocked",
        timeout=1000,
    )

    # Remove relation with both S3 and Azure Storage
    await ops_test.model.applications[APP_NAME].remove_relation(
        f"{APP_NAME}:s3-credentials", f"{charm_versions.s3.application_name}:s3-credentials"
    )
    await ops_test.model.applications[APP_NAME].remove_relation(
        f"{APP_NAME}:azure-credentials",
        f"{charm_versions.azure_storage.application_name}:azure-credentials",
    )

    # wait for active status
    await ops_test.model.wait_for_idle(
        apps=[
            APP_NAME,
            charm_versions.azure_storage.application_name,
            charm_versions.s3.application_name,
        ],
        status="active",
        timeout=1000,
    )


@pytest.mark.abort_on_fail
async def test_relation_to_azure_storage(
    ops_test: OpsTest, charm_versions, namespace, service_account, azure_credentials
):
    logger.info("Relating spark integration hub charm with azure-storage-integrator charm")
    service_account_name = service_account[0]
    secret_data = get_secret_data(
        namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )
    logger.info(f"namespace: {namespace} -> secret_data: {secret_data}")
    assert len(secret_data) == 0

    await ops_test.model.add_relation(charm_versions.azure_storage.application_name, APP_NAME)

    # wait for active status
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, charm_versions.azure_storage.application_name],
        status="active",
        timeout=1000,
    )

    # wait for secret update
    logger.info("Wait for secret update.")
    await juju_sleep(ops_test, APP_NAME, 15)

    secret_data = get_secret_data(
        namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )
    logger.info(f"namespace: {namespace} -> secret_data: {secret_data}")
    assert len(secret_data) > 0
    assert (
        f"spark.hadoop.fs.azure.account.key.{azure_credentials['storage-account']}.dfs.core.windows.net"
        in secret_data
    )


@pytest.mark.abort_on_fail
async def test_add_new_service_account_with_azure_storage(
    ops_test: OpsTest, namespace, service_account, azure_credentials
):
    service_account_name = service_account[0]

    # wait for the update of secrets
    await juju_sleep(ops_test, APP_NAME, 15)

    # check secret
    secret_data = get_secret_data(
        namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )
    assert len(secret_data) > 0
    assert (
        f"spark.hadoop.fs.azure.account.key.{azure_credentials['storage-account']}.dfs.core.windows.net"
        in secret_data
    )


@pytest.mark.abort_on_fail
async def test_add_removal_azure_storage_relation(
    ops_test: OpsTest, namespace, service_account, charm_versions, azure_credentials
):
    service_account_name = service_account[0]

    # wait for the update of secrets
    await juju_sleep(ops_test, APP_NAME, 15)

    # check secret
    secret_data = get_secret_data(
        namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )
    assert len(secret_data) > 0
    assert (
        f"spark.hadoop.fs.azure.account.key.{azure_credentials['storage-account']}.dfs.core.windows.net"
        in secret_data
    )

    await ops_test.model.applications[APP_NAME].remove_relation(
        f"{APP_NAME}:azure-credentials",
        f"{charm_versions.azure_storage.application_name}:azure-credentials",
    )

    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, charm_versions.azure_storage.application_name],
        status="active",
        timeout=300,
        idle_period=30,
    )

    secret_data = get_secret_data(
        namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )
    assert len(secret_data) == 0

    await ops_test.model.add_relation(charm_versions.azure_storage.application_name, APP_NAME)

    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, charm_versions.azure_storage.application_name], timeout=1000
    )

    # wait for active status
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, charm_versions.azure_storage.application_name],
        status="active",
        timeout=1000,
    )

    # wait for secret update
    logger.info("Wait for secret update.")
    await juju_sleep(ops_test, APP_NAME, 15)

    secret_data = get_secret_data(
        namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )
    logger.info(f"namespace: {namespace} -> secret_data: {secret_data}")
    assert len(secret_data) > 0
    assert (
        f"spark.hadoop.fs.azure.account.key.{azure_credentials['storage-account']}.dfs.core.windows.net"
        in secret_data
    )


@pytest.mark.abort_on_fail
async def test_remove_application(
    ops_test: OpsTest, namespace, service_account, azure_credentials, charm_versions
):
    service_account_name = service_account[0]

    # wait for the update of secres
    await juju_sleep(ops_test, APP_NAME, 15)

    # check secret
    secret_data = get_secret_data(
        namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )
    assert len(secret_data) > 0
    assert (
        f"spark.hadoop.fs.azure.account.key.{azure_credentials['storage-account']}.dfs.core.windows.net"
        in secret_data
    )

    logger.info(f"Remove {APP_NAME}")
    await ops_test.model.remove_application(APP_NAME, block_until_done=True, timeout=600)

    await ops_test.model.wait_for_idle(
        apps=[charm_versions.s3.application_name], status="active", timeout=300
    )

    secret_data = get_secret_data(
        namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )
    logger.info(f"secret data: {secret_data}")
    assert len(secret_data) == 0
