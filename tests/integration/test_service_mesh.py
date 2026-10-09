#!/usr/bin/env python3
# Copyright 2026 Canonical Limited
# See LICENSE file for licensing details.

import logging
from pathlib import Path
from time import sleep
from typing import cast

import jubilant
import lightkube
import pytest
import yaml

from constants import ISTIO_AMBIENT_LABEL_KEY, ISTIO_AMBIENT_LABEL_VALUE

from .helpers.cos import (
    assert_metrics_in_pushgateway,
    deploy_observability_setup,
    get_loki_push_endpoint,
)
from .helpers.integration_hub import (
    TEST_CHARM_APP_NAME,
    TEST_CHARM_RELATION_A_NAME,
    assert_eventually,
    deploy_integration_hub_setup,
    deploy_test_charm_setup,
    integration_hub_secret_exists,
)
from .helpers.istio import (
    client_application_authorization_policy_exists,
    client_application_to_driver_authorization_policy_exists,
    deploy_istio_mesh_setup,
    driver_authorization_policy_exists,
    executor_authorization_policy_exists,
)
from .helpers.juju import get_unit_address, get_unit_pod_names
from .helpers.k8s import curl_using_pod, pod_has_labels
from .helpers.spark import (
    SPARK_DRIVER_UI_PORT,
    assert_spark_job_successful,
    cleanup_workload_pods,
    get_spark_driver_pods,
    get_spark_executor_pods,
    run_long_spark_job,
    run_spark_job,
    setup_spark_job,
    spark_service_account_exists,
    wait_for_running_spark_workloads,
)
from .types import IntegrationTestsCharms, S3Info

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]

logger = logging.getLogger(__name__)


def test_deploy_integration_hub(
    juju: jubilant.Juju,
    hub_charm: str,
    charm_versions: IntegrationTestsCharms,
    s3_credentials: S3Info,
    namespace: str,
):
    """Test deploying the integration hub with S3 storage integration."""
    deploy_integration_hub_setup(
        juju=juju,
        hub_charm=hub_charm,
        charm_versions=charm_versions,
        s3_credentials=s3_credentials,
        monitored_service_accounts=f"{namespace}:*",
        trust=True,
    )
    juju.wait(jubilant.all_active)


def test_run_spark_job_before_meshing(lightkube_client: lightkube.Client, service_account: str):
    """Run a Spark job before enabling the service mesh."""
    service_account_name, namespace = service_account
    cleanup_workload_pods(lightkube_client, namespace=namespace)

    setup_spark_job(namespace=namespace, service_account=service_account_name)
    run_spark_job(namespace=namespace, service_account=service_account_name)
    assert_spark_job_successful(lightkube_client, namespace=namespace)

    driver_pods = get_spark_driver_pods(lightkube_client, namespace=namespace)
    executor_pods = get_spark_executor_pods(lightkube_client, namespace=namespace)

    assert not any(
        pod_has_labels(
            lightkube_client,
            pod_name,
            namespace=namespace,
            labels={ISTIO_AMBIENT_LABEL_KEY: ISTIO_AMBIENT_LABEL_VALUE},
        )
        for pod_name in driver_pods
    )
    assert not any(
        pod_has_labels(
            lightkube_client,
            pod_name,
            namespace=namespace,
            labels={ISTIO_AMBIENT_LABEL_KEY: ISTIO_AMBIENT_LABEL_VALUE},
        )
        for pod_name in executor_pods
    )
    cleanup_workload_pods(lightkube_client, namespace=namespace)


def test_access_spark_workloads_from_unmeshed_pod_before_meshing(
    service_account: str, lightkube_client: lightkube.Client
):
    """Test that an unmeshed pod can reach the Spark driver workload before meshing."""
    service_account_name, namespace = service_account

    try:
        run_long_spark_job(namespace=namespace, service_account=service_account_name)
        driver_ip, _ = wait_for_running_spark_workloads(lightkube_client, namespace=namespace)
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
        cleanup_workload_pods(lightkube_client, namespace=namespace)


def test_enable_service_mesh(
    juju: jubilant.Juju, charm_versions: IntegrationTestsCharms, lightkube_client: lightkube.Client
):
    """Enable the service mesh for the integration hub."""
    for pod_name in get_unit_pod_names(lightkube_client, cast(str, juju.model), APP_NAME):
        assert not pod_has_labels(
            lightkube_client,
            namespace=cast(str, juju.model),
            pod_name=pod_name,
            labels={ISTIO_AMBIENT_LABEL_KEY: ISTIO_AMBIENT_LABEL_VALUE},
        )
    deploy_istio_mesh_setup(juju=juju, charm_versions=charm_versions)
    for pod_name in get_unit_pod_names(lightkube_client, cast(str, juju.model), APP_NAME):
        assert pod_has_labels(
            lightkube_client,
            namespace=cast(str, juju.model),
            pod_name=pod_name,
            labels={ISTIO_AMBIENT_LABEL_KEY: ISTIO_AMBIENT_LABEL_VALUE},
        )


def test_run_spark_job_after_meshing(service_account: str, lightkube_client: lightkube.Client):
    """Run a Spark job after enabling the service mesh."""
    service_account_name, namespace = service_account
    cleanup_workload_pods(lightkube_client, namespace=namespace)

    setup_spark_job(namespace=namespace, service_account=service_account_name)
    run_spark_job(namespace=namespace, service_account=service_account_name)
    assert_spark_job_successful(lightkube_client, namespace=namespace)

    driver_pods = get_spark_driver_pods(lightkube_client, namespace=namespace)
    executor_pods = get_spark_executor_pods(lightkube_client, namespace=namespace)

    assert all(
        pod_has_labels(
            lightkube_client,
            pod_name,
            namespace=namespace,
            labels={ISTIO_AMBIENT_LABEL_KEY: ISTIO_AMBIENT_LABEL_VALUE},
        )
        for pod_name in driver_pods
    )
    assert all(
        pod_has_labels(
            lightkube_client,
            pod_name,
            namespace=namespace,
            labels={ISTIO_AMBIENT_LABEL_KEY: ISTIO_AMBIENT_LABEL_VALUE},
        )
        for pod_name in executor_pods
    )
    cleanup_workload_pods(lightkube_client, namespace=namespace)


def test_access_spark_workloads_from_unmeshed_pod_after_meshing(
    service_account: str, lightkube_client: lightkube.Client
):
    """That that an unmeshed pod cannot reach the Spark driver workload after the service mesh is enabled."""
    service_account_name, namespace = service_account

    try:
        run_long_spark_job(namespace=namespace, service_account=service_account_name)
        driver_ip, _ = wait_for_running_spark_workloads(lightkube_client, namespace=namespace)
        curl_driver_process = curl_using_pod(
            namespace=namespace, url=f"http://{driver_ip}:{SPARK_DRIVER_UI_PORT}"
        )
        assert curl_driver_process.returncode != 0, (
            f"Expected failure to curl driver pod, but succeeded: {curl_driver_process.stdout}"
        )
    finally:
        cleanup_workload_pods(lightkube_client, namespace=namespace)


def test_access_spark_workloads_from_meshed_pod_but_unauthorized_after_meshing(
    service_account: str,
    lightkube_client: lightkube.Client,
) -> None:
    """Test that a meshed pod that is unauthorized cannot reach the Spark driver workload after the service mesh is enabled."""
    service_account_name, namespace = service_account

    try:
        run_long_spark_job(namespace=namespace, service_account=service_account_name)
        driver_ip, _ = wait_for_running_spark_workloads(lightkube_client, namespace=namespace)
        curl_driver_process = curl_using_pod(
            namespace=namespace,
            url=f"http://{driver_ip}:{SPARK_DRIVER_UI_PORT}",
            labels={ISTIO_AMBIENT_LABEL_KEY: ISTIO_AMBIENT_LABEL_VALUE},
        )
        assert curl_driver_process.returncode != 0, (
            f"Expected failure to curl driver pod, but succeeded: {curl_driver_process.stdout}"
        )
    finally:
        cleanup_workload_pods(lightkube_client, namespace=namespace)


def test_observability_with_ambient_mesh(
    juju: jubilant.Juju,
    charm_versions: IntegrationTestsCharms,
    service_account: str,
    platform: str,
    lightkube_client: lightkube.Client,
) -> None:
    """Test observability with the ambient service mesh enabled."""
    if platform == "arm64":
        pytest.skip("Skipping observability tests on arm64 platform...")

    deploy_observability_setup(juju, charm_versions)

    pushgateway_address = get_unit_address(juju, charm_versions.pushgateway.application_name)
    with pytest.raises(AssertionError):
        assert_metrics_in_pushgateway(pushgateway_address=pushgateway_address)

    service_account_name, namespace = service_account
    try:
        run_long_spark_job(namespace=namespace, service_account=service_account_name)
        wait_for_running_spark_workloads(lightkube_client, namespace=namespace)
        assert_metrics_in_pushgateway(pushgateway_address=pushgateway_address)
    finally:
        cleanup_workload_pods(lightkube_client, namespace=namespace)

    logger.info(
        "Allowing some time for the workloads to delete their group in pushgateway on job completion"
    )
    sleep(30)

    with pytest.raises(AssertionError):
        assert_metrics_in_pushgateway(pushgateway_address=pushgateway_address)

    sa_name, namespace = service_account
    loki_endpoint = get_loki_push_endpoint(
        juju, "logging", charm_versions.grafana_agent.application_name
    )
    assert integration_hub_secret_exists(
        lightkube_client,
        namespace,
        sa_name,
        with_properties={
            "spark.executorEnv.LOKI_URL": loki_endpoint,
            "spark.kubernetes.driverEnv.LOKI_URL": loki_endpoint,
        },
    ), (
        f"Integration hub secret with required properties for service account '{sa_name}' does not exist"
    )


def test_integration_with_client_app(
    juju: jubilant.Juju, namespace: str, test_charm: str | Path, lightkube_client: lightkube.Client
):
    """Deploy and integrate test charm and assert the existence of related resources."""
    deploy_test_charm_setup(
        juju=juju,
        test_charm=test_charm,
        spark_workload_namespace=namespace,
    )

    logger.info(
        "Asserting the existence of service account, integration hub secret, and authorization policies"
    )
    assert spark_service_account_exists(namespace, "sa1")
    assert integration_hub_secret_exists(lightkube_client, namespace, "sa1"), (
        "Integration hub secret for service account 'sa1' does not exist"
    )
    assert driver_authorization_policy_exists(lightkube_client, namespace, "sa1"), (
        "Driver authorization policy for service account 'sa1' does not exist"
    )
    assert executor_authorization_policy_exists(lightkube_client, namespace, "sa1"), (
        "Executor authorization policy for service account 'sa1' does not exist"
    )
    assert client_application_authorization_policy_exists(
        lightkube_client,
        workload_namespace=namespace,
        workload_service_account="sa1",
        client_app_namespace=cast(str, juju.model),
        client_app_service_account=TEST_CHARM_APP_NAME,
    ), "Client application authorization policy for service account 'sa1' does not exist"
    assert client_application_to_driver_authorization_policy_exists(
        lightkube_client,
        workload_namespace=namespace,
        workload_service_account="sa1",
        client_app_namespace=cast(str, juju.model),
        client_app_service_account=TEST_CHARM_APP_NAME,
    ), "Client-application-to-driver authorization policy for service account 'sa1' does not exist"


def test_removing_service_mesh_relation_deletes_authorization_policies(
    juju: jubilant.Juju,
    charm_versions: IntegrationTestsCharms,
    namespace: str,
    lightkube_client: lightkube.Client,
):
    """Test that removing the service mesh relation deletes all related authorization policies and that re-adding the relation restores them."""
    logger.info("Removing service mesh relation from integration hub charm")
    juju.remove_relation(
        f"{APP_NAME}:service-mesh", f"{charm_versions.istio_beacon.application_name}:service-mesh"
    )
    juju.wait(
        lambda status: (
            jubilant.all_agents_idle(status)
            and jubilant.all_active(status, APP_NAME, charm_versions.istio_beacon.application_name)
        ),
        delay=15,
    )
    assert not driver_authorization_policy_exists(lightkube_client, namespace, "sa1"), (
        "Driver authorization policy for service account 'sa1' should not exist after removing the relation"
    )
    assert not executor_authorization_policy_exists(lightkube_client, namespace, "sa1"), (
        "Executor authorization policy for service account 'sa1' should not exist after removing the relation"
    )
    assert not client_application_authorization_policy_exists(
        lightkube_client,
        workload_namespace=namespace,
        workload_service_account="sa1",
        client_app_namespace=cast(str, juju.model),
        client_app_service_account=TEST_CHARM_APP_NAME,
    ), (
        "Client application authorization policy for service account 'sa1' should not exist after removing the relation"
    )
    assert not client_application_to_driver_authorization_policy_exists(
        lightkube_client,
        workload_namespace=namespace,
        workload_service_account="sa1",
        client_app_namespace=cast(str, juju.model),
        client_app_service_account=TEST_CHARM_APP_NAME,
    ), (
        "Client-application-to-driver authorization policy for service account 'sa1' should not exist after removing the relation"
    )

    logger.info("Adding service mesh relation to integration hub charm again...")
    juju.integrate(
        f"{APP_NAME}:service-mesh", f"{charm_versions.istio_beacon.application_name}:service-mesh"
    )
    juju.wait(
        lambda status: (
            jubilant.all_agents_idle(status)
            and jubilant.all_active(status, APP_NAME, charm_versions.istio_beacon.application_name)
        ),
        delay=15,
    )
    assert driver_authorization_policy_exists(lightkube_client, namespace, "sa1"), (
        "Driver authorization policy for service account 'sa1' does not exist"
    )
    assert executor_authorization_policy_exists(lightkube_client, namespace, "sa1"), (
        "Executor authorization policy for service account 'sa1' does not exist"
    )
    assert client_application_authorization_policy_exists(
        lightkube_client,
        workload_namespace=namespace,
        workload_service_account="sa1",
        client_app_namespace=cast(str, juju.model),
        client_app_service_account=TEST_CHARM_APP_NAME,
    ), "Client application authorization policy for service account 'sa1' does not exist"
    assert client_application_to_driver_authorization_policy_exists(
        lightkube_client,
        workload_namespace=namespace,
        workload_service_account="sa1",
        client_app_namespace=cast(str, juju.model),
        client_app_service_account=TEST_CHARM_APP_NAME,
    ), "Client-application-to-driver authorization policy for service account 'sa1' does not exist"


def test_remove_client_app_integration(
    juju: jubilant.Juju, namespace: str, lightkube_client: lightkube.Client
):
    logger.info("Removing relation between integration hub and test charm")
    juju.remove_relation(APP_NAME, f"{TEST_CHARM_APP_NAME}:{TEST_CHARM_RELATION_A_NAME}")
    juju.wait(
        lambda status: (
            jubilant.all_active(status, TEST_CHARM_APP_NAME, APP_NAME)
            and jubilant.all_agents_idle(status)
        ),
        delay=30,
    )
    logger.info(
        "Asserting the cleanup of service account, integration hub secret, and authorization policies"
    )
    assert_eventually(
        lambda: not spark_service_account_exists(namespace, "sa1"),
        message="Spark service account 'sa1' should not exist after removing the relation",
    )
    assert_eventually(
        lambda: not integration_hub_secret_exists(lightkube_client, namespace, "sa1"),
        message="Integration hub secret for service account 'sa1' should not exist after removing the relation",
    )
    assert_eventually(
        lambda: not driver_authorization_policy_exists(lightkube_client, namespace, "sa1"),
        message="Driver authorization policy for service account 'sa1' should not exist after removing the relation",
    )
    assert_eventually(
        lambda: not executor_authorization_policy_exists(lightkube_client, namespace, "sa1"),
        message="Executor authorization policy for service account 'sa1' should not exist after removing the relation",
    )
    assert_eventually(
        lambda: (
            not client_application_authorization_policy_exists(
                lightkube_client,
                workload_namespace=namespace,
                workload_service_account="sa1",
                client_app_namespace=cast(str, juju.model),
                client_app_service_account=TEST_CHARM_APP_NAME,
            )
        ),
        message="Client application authorization policy for service account 'sa1' should not exist after removing the relation",
    )
    assert_eventually(
        lambda: (
            not client_application_to_driver_authorization_policy_exists(
                lightkube_client,
                workload_namespace=namespace,
                workload_service_account="sa1",
                client_app_namespace=cast(str, juju.model),
                client_app_service_account=TEST_CHARM_APP_NAME,
            )
        ),
        message="Client-application-to-driver authorization policy for service account 'sa1' should not exist after removing the relation",
    )


def test_disable_service_mesh(
    juju: jubilant.Juju, charm_versions: IntegrationTestsCharms, lightkube_client: lightkube.Client
):
    """Disable the service mesh for the Integration Hub charm."""
    logger.info("Disabling ambient mesh for Integration hub charm")
    juju.remove_relation(
        f"{APP_NAME}:service-mesh", f"{charm_versions.istio_beacon.application_name}:service-mesh"
    )
    juju.wait(
        lambda status: (
            jubilant.all_agents_idle(status)
            and jubilant.all_active(status, APP_NAME, charm_versions.istio_beacon.application_name)
        ),
        delay=15,
    )
    logger.info("Asserting the istio labels are removed from integration hub pods")
    for pod_name in get_unit_pod_names(lightkube_client, cast(str, juju.model), APP_NAME):
        assert not pod_has_labels(
            lightkube_client,
            namespace=cast(str, juju.model),
            pod_name=pod_name,
            labels={ISTIO_AMBIENT_LABEL_KEY: ISTIO_AMBIENT_LABEL_VALUE},
        )


def test_run_spark_job_after_unmeshing(service_account: str, lightkube_client: lightkube.Client):
    """Run a Spark job after disabling the service mesh."""
    service_account_name, namespace = service_account
    cleanup_workload_pods(lightkube_client, namespace=namespace)

    setup_spark_job(namespace=namespace, service_account=service_account_name)
    run_spark_job(namespace=namespace, service_account=service_account_name)
    assert_spark_job_successful(lightkube_client, namespace=namespace)

    driver_pods = get_spark_driver_pods(lightkube_client, namespace=namespace)
    executor_pods = get_spark_executor_pods(lightkube_client, namespace=namespace)

    assert not any(
        pod_has_labels(
            lightkube_client,
            pod_name,
            namespace=namespace,
            labels={ISTIO_AMBIENT_LABEL_KEY: ISTIO_AMBIENT_LABEL_VALUE},
        )
        for pod_name in driver_pods
    )
    assert not any(
        pod_has_labels(
            lightkube_client,
            pod_name,
            namespace=namespace,
            labels={ISTIO_AMBIENT_LABEL_KEY: ISTIO_AMBIENT_LABEL_VALUE},
        )
        for pod_name in executor_pods
    )
    cleanup_workload_pods(lightkube_client, namespace=namespace)


def test_access_spark_workloads_from_unmeshed_pod_after_unmeshing(
    service_account: str, lightkube_client: lightkube.Client
):
    """Test that Spark workloads are accessible from an unmeshed pod after disabling the service mesh."""
    service_account_name, namespace = service_account

    try:
        run_long_spark_job(namespace=namespace, service_account=service_account_name)
        driver_ip, _ = wait_for_running_spark_workloads(lightkube_client, namespace=namespace)
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
        cleanup_workload_pods(lightkube_client, namespace=namespace)
