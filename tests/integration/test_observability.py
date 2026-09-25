#!/usr/bin/env python3
# Copyright 2025 Canonical Limited
# See LICENSE file for licensing details.

import datetime
import json
import logging
import time
import urllib.request
from pathlib import Path

import jubilant
import pytest
import yaml
from spark8t.utils import K8sSecretKeySerializer
from tenacity import retry, stop_after_attempt, wait_fixed

from .helpers.cos import assert_metrics_in_pushgateway, deploy_observability_setup
from .helpers.integration_hub import (
    deploy_integration_hub_setup,
    get_integration_hub_secret_data,
    integration_hub_secret_exists,
)
from .helpers.juju import get_unit_address
from .helpers.spark import (
    cleanup_workload_pods,
    run_long_spark_job,
    wait_for_running_spark_workloads,
)
from .types import IntegrationTestsCharms, S3Info

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
    juju: jubilant.Juju,
    charm_versions: IntegrationTestsCharms,
    hub_charm: str | Path,
    s3_credentials: S3Info,
) -> None:
    deploy_integration_hub_setup(
        juju=juju,
        hub_charm=hub_charm,
        charm_versions=charm_versions,
        s3_credentials=s3_credentials,
        trust=True,
    )
    juju.wait(jubilant.all_active)


def test_deploy_cos_charms(
    juju: jubilant.Juju,
    charm_versions: IntegrationTestsCharms,
) -> None:
    deploy_observability_setup(
        juju=juju,
        charm_versions=charm_versions,
    )


def test_relation_with_pushgateway(
    juju: jubilant.Juju,
    charm_versions: IntegrationTestsCharms,
    service_account: tuple[str, str],
    platform: str,
) -> None:
    """Test relation with prometheus pushgateway.

    Assert on the unit status and on the presence/absence of the metrics.
    """
    if platform == "arm64":
        pytest.skip("Skipping observability tests on arm64 platform...")

    service_account_name, namespace = service_account
    juju.config(APP_NAME, {"monitored-service-accounts": f"{namespace}:{service_account_name}"})
    juju.wait(
        lambda status: jubilant.all_active(status, APP_NAME),
        delay=15,
    )

    assert integration_hub_secret_exists(namespace, service_account_name)
    secret_data = get_integration_hub_secret_data(namespace, service_account_name)
    assert any("spark.metrics.conf" in key for key in secret_data.keys())

    pushgateway_address = get_unit_address(juju, charm_versions.pushgateway.application_name)
    with pytest.raises(AssertionError):
        assert_metrics_in_pushgateway(pushgateway_address=pushgateway_address)

    service_account_name, namespace = service_account
    try:
        run_long_spark_job(namespace=namespace, service_account=service_account_name)
        wait_for_running_spark_workloads(namespace=namespace)
        assert_metrics_in_pushgateway(pushgateway_address=pushgateway_address)
    finally:
        cleanup_workload_pods(namespace=namespace)

    logger.info(
        "Allowing some time for the workloads to delete their group in pushgateway on job completion"
    )
    time.sleep(10)

    with pytest.raises(AssertionError):
        assert_metrics_in_pushgateway(pushgateway_address=pushgateway_address)

    juju.remove_relation(APP_NAME, charm_versions.pushgateway.application_name)
    juju.wait(
        lambda status: jubilant.all_active(
            status, charm_versions.pushgateway.application_name, APP_NAME
        ),
        delay=15,
    )
    secret_data = get_integration_hub_secret_data(namespace, service_account_name)
    assert not any("spark.metrics.conf" in key for key in secret_data.keys())


def test_relation_with_logging(
    juju: jubilant.Juju, service_account: tuple[str, str], charm_versions: IntegrationTestsCharms
) -> None:
    service_account_name, namespace = service_account
    juju.config(APP_NAME, {"monitored-service-accounts": f"{namespace}:{service_account_name}"})
    juju.wait(
        lambda status: jubilant.all_active(status, APP_NAME),
        delay=15,
    )

    secret_data = get_integration_hub_secret_data(namespace, service_account_name)
    serializer = K8sSecretKeySerializer()
    assert serializer.serialize("spark.executorEnv.LOKI_URL") in secret_data
    assert serializer.serialize("spark.kubernetes.driverEnv.LOKI_URL") in secret_data

    logger.info(
        "Remove relation between %s and %s",
        APP_NAME,
        charm_versions.grafana_agent.application_name,
    )
    juju.remove_relation(APP_NAME, charm_versions.grafana_agent.application_name)
    juju.wait(jubilant.all_agents_idle, delay=5)

    secret_data = get_integration_hub_secret_data(namespace, service_account_name)
    assert serializer.serialize("spark.executorEnv.LOKI_URL") not in secret_data
    assert serializer.serialize("spark.kubernetes.driverEnv.LOKI_URL") not in secret_data
