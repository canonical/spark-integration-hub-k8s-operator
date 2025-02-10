#!/usr/bin/env python3
# Copyright 2025 Canonical Limited
# See LICENSE file for licensing details.


import asyncio
import datetime
import json
import logging
import subprocess
import urllib.request
from pathlib import Path

import pytest
import yaml
from pytest_operator.plugin import OpsTest
from tenacity import retry, stop_after_attempt, wait_fixed

from .helpers import (
    BUCKET_NAME,
    fetch_action_sync_s3_credentials,
    get_secret_data,
    juju_sleep,
    setup_s3_bucket_for_sch_server,
)

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]
CONTAINER_NAME = "test-container"
SECRET_NAME_PREFIX = "integrator-hub-conf-"


@retry(
    wait=wait_fixed(5),
    stop=stop_after_attempt(120),
    reraise=True,
)
def check_metrics(address: str) -> None:
    metrics = json.loads(urllib.request.urlopen(f"http://{address}:9091/api/v1/metrics").read())

    logger.info(f"Metrics: {metrics} at time: {datetime.datetime.now()}")

    assert len(metrics["data"]) > 0


@pytest.mark.abort_on_fail
async def test_build_and_deploy(ops_test: OpsTest, charm_versions, service_account, namespace):
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

    logger.info("Deploying Spark Integration hub charm and s3-integrator charm")

    # Deploy the charm and wait for waiting status
    await asyncio.gather(
        ops_test.model.deploy(**charm_versions.s3.deploy_dict()),
        ops_test.model.deploy(
            charm,
            resources=resources,
            application_name=APP_NAME,
            num_units=1,
            series="jammy",
            trust=True,
        ),
    )

    logger.info("Waiting for s3-integrator and integration hub charms to be idle...")
    await ops_test.model.wait_for_idle(
        apps=[
            APP_NAME,
            charm_versions.s3.application_name,
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

    logger.info("Deploying the grafana-agent-k8s charm")
    await ops_test.model.deploy(**charm_versions.grafana_agent.deploy_dict())
    logger.debug(
        "Waiting for %s to by in blocked state", charm_versions.grafana_agent.application_name
    )
    # Note(rgildein): The grafana-agent-k8s charm is in blocked state, since we are not deploying whole cos.
    await ops_test.model.wait_for_idle(
        apps=[charm_versions.grafana_agent.application_name], status="blocked"
    )

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
async def test_relation_to_pushgateway(
    ops_test: OpsTest, charm_versions, namespace, service_account
):
    """Test relation with prometheus pushgateway.

    Assert on the unit status and on the presence/absence of the metrics.
    """
    service_account_name, namespace = service_account
    setup_spark_output = subprocess.check_output(
        f"./tests/integration/setup/setup_spark.sh {service_account_name} {namespace}",
        shell=True,
        stderr=None,
    ).decode("utf-8")
    logger.info(f"Setup spark output:\n{setup_spark_output}")

    logger.info("Relating spark integration hub charm with s3-integrator charm")
    logger.info(f"Test with namespace: {namespace}")
    await ops_test.model.deploy(**charm_versions.pushgateway.deploy_dict())

    await ops_test.model.wait_for_idle(
        apps=[charm_versions.pushgateway.application_name], timeout=1000, status="active"
    )

    await ops_test.model.add_relation(charm_versions.pushgateway.application_name, APP_NAME)

    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, charm_versions.pushgateway.application_name],
        status="active",
        timeout=1000,
    )

    await juju_sleep(ops_test, APP_NAME, 15)

    secret_data = get_secret_data(
        namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )
    logger.info(f"namespace: {namespace} -> secret_data: {secret_data}")

    conf_prop = False
    for key in secret_data.keys():
        if "spark.metrics.conf" in key:
            conf_prop = True
            break
    assert conf_prop

    status = await ops_test.model.get_status()
    address = status["applications"][charm_versions.pushgateway.application_name]["units"][
        f"{charm_versions.pushgateway.application_name}/0"
    ]["address"]

    metrics = json.loads(urllib.request.urlopen(f"http://{address}:9091/api/v1/metrics").read())

    assert len(metrics["data"]) == 0

    logger.info("Executing Spark job in async mode")

    await juju_sleep(ops_test, APP_NAME, 5)
    proc = await asyncio.create_subprocess_exec(
        "./tests/integration/setup/run_spark_job.sh",
        service_account_name,
        namespace,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    logger.info("Verifying metrics are present in the pushgateway while job is running")

    check_metrics(address=address)

    stdout, stderr = await proc.communicate()
    logger.info(f"spark job stdout :\n{stdout}")
    logger.info(f"Spark job stderr :\n{stderr}")

    logger.info("Spark job has ended!")
    await juju_sleep(ops_test, APP_NAME, 15)
    logger.info("Check that metrics are deleted from the prometheus pushgateway")
    metrics = json.loads(urllib.request.urlopen(f"http://{address}:9091/api/v1/metrics").read())

    logger.info(f"Metrics after job ended: {metrics}")

    assert len(metrics["data"]) == 0

    await ops_test.model.applications[APP_NAME].remove_relation(
        f"{APP_NAME}:cos", f"{charm_versions.pushgateway.application_name}:push-endpoint"
    )

    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, charm_versions.pushgateway.application_name],
        status="active",
        timeout=300,
        idle_period=30,
    )

    secret_data = get_secret_data(
        namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )

    for key in secret_data.keys():
        if "spark.metrics.conf" in key:
            assert False


@pytest.mark.abort_on_fail
async def test_integrate_logging_relation(ops_test: OpsTest, service_account, charm_versions):
    """Test integrate logging relation."""
    service_account_name, namespace = service_account
    setup_spark_output = subprocess.check_output(
        f"./tests/integration/setup/setup_spark.sh {service_account_name} {namespace}",
        shell=True,
        stderr=None,
    ).decode("utf-8")
    logger.info(f"Setup spark output:\n{setup_spark_output}")

    logger.info(
        "Integrate %s with %s through logging relation",
        APP_NAME,
        charm_versions.grafana_agent.application_name,
    )
    await ops_test.model.integrate(
        f"{charm_versions.grafana_agent.application_name}:logging-provider", f"{APP_NAME}:logging"
    )

    # wait for the update of secrets
    await juju_sleep(ops_test, APP_NAME, 15)

    # check secret
    secret_data = get_secret_data(
        namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )
    # Note(rgildein): Double underscores are used in secrets, but only one will be present in POD.
    assert "spark.executorEnv.LOKI__URL" in secret_data
    assert "spark.kubernetes.driverEnv.LOKI__URL" in secret_data


@pytest.mark.abort_on_fail
async def test_remove_logging_relation(ops_test: OpsTest, service_account, charm_versions):
    """Test remove logging relation."""
    service_account_name, namespace = service_account
    setup_spark_output = subprocess.check_output(
        f"./tests/integration/setup/setup_spark.sh {service_account_name} {namespace}",
        shell=True,
        stderr=None,
    ).decode("utf-8")
    logger.info(f"Setup spark output:\n{setup_spark_output}")

    logger.info(
        "Remove relation between %s and %s",
        APP_NAME,
        charm_versions.grafana_agent.application_name,
    )
    await ops_test.model.applications[APP_NAME].remove_relation(
        f"{charm_versions.grafana_agent.application_name}:logging-provider", f"{APP_NAME}:logging"
    )

    # wait for the update of secrets
    await juju_sleep(ops_test, APP_NAME, 15)

    # check secret
    secret_data = get_secret_data(
        namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )
    # Note(rgildein): Double underscores are used in secrets, but only one will be present in POD.
    assert "spark.executorEnv.LOKI__URL" not in secret_data
    assert "spark.kubernetes.driverEnv.LOKI__URL" not in secret_data
