#!/usr/bin/env python3
# Copyright 2026 Canonical Limited
# See LICENSE file for licensing details.

import logging
from pathlib import Path
from typing import cast

import jubilant
import yaml

from .types import IntegrationTestsCharms, S3Info
from .utils.integration_hub import deploy_integration_hub_setup
from .utils.spark import (
    SPARK_DRIVER_UI_PORT,
    assert_spark_job_successful,
    cleanup_workload_pods,
    get_spark_driver_pods,
    get_spark_executor_pods,
    run_long_spark_job,
    run_spark_job,
    setup_spark_job,
    wait_for_running_spark_workloads,
)
from .utils.k8s import curl_using_pod, pod_has_labels
from .utils.juju import get_unit_pod_names
from .utils.istio import deploy_istio_mesh_setup

from constants import ISTIO_AMBIENT_LABEL_KEY, ISTIO_AMBIENT_LABEL_VALUE

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]

logger = logging.getLogger(__name__)


def test_deploy_integration_hub(
    juju: jubilant.Juju,
    hub_charm: str,
    charm_versions: IntegrationTestsCharms,
    s3_credentials: S3Info,
):
    """Test deploying the integration hub with S3 storage integration."""
    deploy_integration_hub_setup(
        juju=juju,
        hub_charm=hub_charm,
        charm_versions=charm_versions,
        s3_credentials=s3_credentials,
        trust=True,
    )
    juju.wait(jubilant.all_active)


def test_run_spark_job_before_meshing(service_account: str):
    """Run a Spark job before enabling the service mesh."""
    service_account_name, namespace = service_account
    cleanup_workload_pods(namespace=namespace)

    setup_spark_job(namespace=namespace, service_account=service_account_name)
    run_spark_job(namespace=namespace, service_account=service_account_name)
    assert_spark_job_successful(namespace=namespace)

    driver_pods = get_spark_driver_pods(namespace=namespace)
    executor_pods = get_spark_executor_pods(namespace=namespace)

    assert not any(
        pod_has_labels(
            pod_name,
            namespace=namespace,
            labels={ISTIO_AMBIENT_LABEL_KEY: ISTIO_AMBIENT_LABEL_VALUE},
        )
        for pod_name in driver_pods
    )
    assert not any(
        pod_has_labels(
            pod_name,
            namespace=namespace,
            labels={ISTIO_AMBIENT_LABEL_KEY: ISTIO_AMBIENT_LABEL_VALUE},
        )
        for pod_name in executor_pods
    )
    cleanup_workload_pods(namespace=namespace)


def test_access_spark_workloads_from_unmeshed_pod_before_meshing(service_account: str):
    """An unmeshed pod can reach the Spark driver workload before meshing."""
    service_account_name, namespace = service_account

    try:
        run_long_spark_job(namespace=namespace, service_account=service_account_name)
        driver_ip, _ = wait_for_running_spark_workloads(namespace=namespace)
        curl_driver_process = curl_using_pod(
            namespace=namespace, url=f"http://{driver_ip}:{SPARK_DRIVER_UI_PORT}"
        )
        assert curl_driver_process.returncode == 0, (
            f"Failed to curl driver pod: {curl_driver_process.stderr}"
        )
        assert curl_driver_process.stdout.endswith("302"), (
            f"Unexpected HTTP status code: {curl_driver_process.stdout}"
        )
    finally:
        cleanup_workload_pods(namespace=namespace)


def test_enable_service_mesh(juju: jubilant.Juju, charm_versions: IntegrationTestsCharms):
    """Enable the service mesh for the integration hub."""
    for pod_name in get_unit_pod_names(cast(str, juju.model), APP_NAME):
        assert not pod_has_labels(
            namespace=cast(str, juju.model),
            pod_name=pod_name,
            labels={ISTIO_AMBIENT_LABEL_KEY: ISTIO_AMBIENT_LABEL_VALUE},
        )
    deploy_istio_mesh_setup(juju=juju, charm_versions=charm_versions)
    for pod_name in get_unit_pod_names(cast(str, juju.model), APP_NAME):
        assert pod_has_labels(
            namespace=cast(str, juju.model),
            pod_name=pod_name,
            labels={ISTIO_AMBIENT_LABEL_KEY: ISTIO_AMBIENT_LABEL_VALUE},
        )


def test_run_spark_job_after_meshing(service_account: str):
    """Run a Spark job after enabling the service mesh."""
    service_account_name, namespace = service_account
    cleanup_workload_pods(namespace=namespace)

    setup_spark_job(namespace=namespace, service_account=service_account_name)
    run_spark_job(namespace=namespace, service_account=service_account_name)
    assert_spark_job_successful(namespace=namespace)

    driver_pods = get_spark_driver_pods(namespace=namespace)
    executor_pods = get_spark_executor_pods(namespace=namespace)

    assert all(
        pod_has_labels(
            pod_name,
            namespace=namespace,
            labels={ISTIO_AMBIENT_LABEL_KEY: ISTIO_AMBIENT_LABEL_VALUE},
        )
        for pod_name in driver_pods
    )
    assert all(
        pod_has_labels(
            pod_name,
            namespace=namespace,
            labels={ISTIO_AMBIENT_LABEL_KEY: ISTIO_AMBIENT_LABEL_VALUE},
        )
        for pod_name in executor_pods
    )
    cleanup_workload_pods(namespace=namespace)


def test_access_spark_workloads_from_unmeshed_pod_after_meshing(service_account: str):
    """An unmeshed pod can-not reach the Spark driver workload after the service mesh is enabled."""
    service_account_name, namespace = service_account

    try:
        run_long_spark_job(namespace=namespace, service_account=service_account_name)
        driver_ip, _ = wait_for_running_spark_workloads(namespace=namespace)
        curl_driver_process = curl_using_pod(
            namespace=namespace, url=f"http://{driver_ip}:{SPARK_DRIVER_UI_PORT}"
        )
        assert curl_driver_process.returncode != 0, (
            f"Expected failure to curl driver pod, but succeeded: {curl_driver_process.stdout}"
        )
    finally:
        cleanup_workload_pods(namespace=namespace)


def test_access_spark_workloads_from_meshed_pod_but_unauthorized_after_meshing(
    service_account: str,
):
    """A meshed pod that is unauthorized cannot reach the Spark driver workload after the service mesh is enabled."""
    service_account_name, namespace = service_account

    try:
        run_long_spark_job(namespace=namespace, service_account=service_account_name)
        driver_ip, _ = wait_for_running_spark_workloads(namespace=namespace)
        curl_driver_process = curl_using_pod(
            namespace=namespace,
            url=f"http://{driver_ip}:{SPARK_DRIVER_UI_PORT}",
            labels={ISTIO_AMBIENT_LABEL_KEY: ISTIO_AMBIENT_LABEL_VALUE},
        )
        assert curl_driver_process.returncode != 0, (
            f"Expected failure to curl driver pod, but succeeded: {curl_driver_process.stdout}"
        )
    finally:
        cleanup_workload_pods(namespace=namespace)


def test_observability_with_ambient_mesh():
    pass


def test_deploy_and_integrate_client_app():
    pass


def test_access_spark_workload_from_client_app_after_meshing():
    pass


def test_disable_service_mesh(juju: jubilant.Juju, charm_versions: IntegrationTestsCharms):
    logger.info("Disabling ambient mesh for Integration hub charm")
    juju.remove_relation(
        f"{APP_NAME}:service-mesh", f"{charm_versions.istio_beacon.application_name}:service-mesh"
    )
    juju.wait(
        lambda status: jubilant.all_agents_idle(status) and jubilant.all_active(status), delay=5
    )
    for pod_name in get_unit_pod_names(cast(str, juju.model), APP_NAME):
        assert not pod_has_labels(
            namespace=cast(str, juju.model),
            pod_name=pod_name,
            labels={ISTIO_AMBIENT_LABEL_KEY: ISTIO_AMBIENT_LABEL_VALUE},
        )


def test_run_spark_job_after_unmeshing(service_account: str):
    """Run a Spark job after disabling the service mesh."""
    service_account_name, namespace = service_account
    cleanup_workload_pods(namespace=namespace)

    setup_spark_job(namespace=namespace, service_account=service_account_name)
    run_spark_job(namespace=namespace, service_account=service_account_name)
    assert_spark_job_successful(namespace=namespace)

    driver_pods = get_spark_driver_pods(namespace=namespace)
    executor_pods = get_spark_executor_pods(namespace=namespace)

    assert not any(
        pod_has_labels(
            pod_name,
            namespace=namespace,
            labels={ISTIO_AMBIENT_LABEL_KEY: ISTIO_AMBIENT_LABEL_VALUE},
        )
        for pod_name in driver_pods
    )
    assert not any(
        pod_has_labels(
            pod_name,
            namespace=namespace,
            labels={ISTIO_AMBIENT_LABEL_KEY: ISTIO_AMBIENT_LABEL_VALUE},
        )
        for pod_name in executor_pods
    )
    cleanup_workload_pods(namespace=namespace)


def test_access_spark_workloads_from_unmeshed_pod_after_unmeshing(service_account: str):
    service_account_name, namespace = service_account

    try:
        run_long_spark_job(namespace=namespace, service_account=service_account_name)
        driver_ip, _ = wait_for_running_spark_workloads(namespace=namespace)
        curl_driver_process = curl_using_pod(
            namespace=namespace, url=f"http://{driver_ip}:{SPARK_DRIVER_UI_PORT}"
        )
        assert curl_driver_process.returncode == 0, (
            f"Failed to curl driver pod: {curl_driver_process.stderr}"
        )
        assert curl_driver_process.stdout.endswith("302"), (
            f"Unexpected HTTP status code: {curl_driver_process.stdout}"
        )
    finally:
        cleanup_workload_pods(namespace=namespace)
