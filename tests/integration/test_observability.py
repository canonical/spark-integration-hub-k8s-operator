#!/usr/bin/env python3
# Copyright 2025 Canonical Limited
# See LICENSE file for licensing details.

import datetime
import json
import logging
import subprocess
import time
import urllib.request
from pathlib import Path

import jubilant
import yaml
from tenacity import retry, stop_after_attempt, wait_fixed

from .helpers import (
    get_address,
    get_secret_data,
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


def test_deploy_hub_with_s3_relation(
    juju: jubilant.Juju, charm_versions, deploy_hub_charm, deploy_s3_integrator_charm
) -> None:
    juju.wait(
        lambda status: jubilant.all_active(status, APP_NAME, charm_versions.s3.application_name)
    )

    # Relate S3 integrator with Spark Integration Hub
    juju.integrate(
        APP_NAME,
        charm_versions.s3.application_name,
    )
    juju.wait(jubilant.all_active)


def test_deploy_cos_charms(
    juju: jubilant.Juju,
    charm_versions,
) -> None:
    logger.info("Deploying the grafana-agent-k8s charm")
    juju.deploy(**charm_versions.grafana_agent.deploy_dict())
    juju.wait(
        lambda status: jubilant.all_blocked(status, charm_versions.grafana_agent.application_name)
    )

    logger.info("Deploying the prometheus-pushgateway-k8s charm")
    juju.deploy(**charm_versions.pushgateway.deploy_dict())
    juju.wait(
        lambda status: jubilant.all_active(status, charm_versions.pushgateway.application_name)
    )


def test_relation_with_pushgateway(juju: jubilant.Juju, charm_versions, service_account) -> None:
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

    logger.info("Relating spark integration hub charm with pushgateway charm")
    juju.integrate(
        APP_NAME,
        charm_versions.pushgateway.application_name,
    )
    juju.wait(
        lambda status: jubilant.all_active(
            status, APP_NAME, charm_versions.pushgateway.application_name
        ),
        delay=5,
    )

    # Verify that appropriate Spark properties for the pushgateway configuration have
    # been added to the secret data.
    secret_data = get_secret_data(
        namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )
    logger.info(f"namespace: {namespace} -> secret_data: {secret_data}")

    assert isinstance(secret_data, dict)
    assert any("spark.metrics.conf" in key for key in secret_data.keys())

    pushgateway_address = get_address(juju, f"{charm_versions.pushgateway.application_name}/0")
    metrics = json.loads(
        urllib.request.urlopen(f"http://{pushgateway_address}:9091/api/v1/metrics").read()
    )
    assert len(metrics["data"]) == 0

    # Wait for some time for the changes in secrets to be reflected
    logger.info("Waiting for 5 seconds...")
    time.sleep(5)

    logger.info("Executing Spark job...")
    proc = subprocess.Popen(
        ["./tests/integration/setup/run_spark_job.sh", service_account_name, namespace],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    logger.info("Verifying metrics are present in the pushgateway while job is running")

    check_metrics(address=pushgateway_address)

    stdout, stderr = proc.communicate()

    logger.info(f"Spark job stdout:\n{stdout}")
    logger.info(f"Spark job stderr:\n{stderr}")
    logger.info("Spark job has ended!")

    logger.info("Waiting for 5 seconds...")
    time.sleep(5)

    logger.info("Check that metrics are deleted from the prometheus pushgateway")
    metrics = json.loads(
        urllib.request.urlopen(f"http://{pushgateway_address}:9091/api/v1/metrics").read()
    )
    logger.info(f"Metrics after job ended: {metrics}")
    assert len(metrics["data"]) == 0

    juju.remove_relation(APP_NAME, charm_versions.pushgateway.application_name)
    juju.wait(
        lambda status: jubilant.all_active(
            status, charm_versions.pushgateway.application_name, APP_NAME
        ),
        delay=5,
    )

    secret_data = get_secret_data(
        namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )
    logger.info(f"namespace: {namespace} -> secret_data: {secret_data}")
    assert isinstance(secret_data, dict)
    assert not any("spark.metrics.conf" in key for key in secret_data.keys())


def test_relation_with_logging(juju: jubilant.Juju, service_account, charm_versions) -> None:
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
    juju.integrate(
        f"{charm_versions.grafana_agent.application_name}:logging-provider",
        f"{APP_NAME}:logging",
    )
    juju.wait(jubilant.all_agents_idle, delay=5)

    secret_data = get_secret_data(
        namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )
    # Note(rgildein): Double underscores are used in secrets, but only one will be present in POD.
    assert "spark.executorEnv.LOKI__URL" in secret_data
    assert "spark.kubernetes.driverEnv.LOKI__URL" in secret_data

    logger.info(
        "Remove relation between %s and %s",
        APP_NAME,
        charm_versions.grafana_agent.application_name,
    )
    juju.remove_relation(APP_NAME, charm_versions.grafana_agent.application_name)
    juju.wait(jubilant.all_agents_idle, delay=5)

    secret_data = get_secret_data(
        namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )
    # Note(rgildein): Double underscores are used in secrets, but only one will be present in POD.
    assert "spark.executorEnv.LOKI__URL" not in secret_data
    assert "spark.kubernetes.driverEnv.LOKI__URL" not in secret_data
