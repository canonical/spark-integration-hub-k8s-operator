#!/usr/bin/env python3
# Copyright 2026 Canonical Limited
# See LICENSE file for licensing details.

import logging
import subprocess

from tenacity import retry, stop_after_attempt, wait_fixed

from .k8s import (
    delete_pod,
    get_pod_ip,
    get_pod_logs,
    get_pod_phase,
    get_pods_by_label,
    wait_for_pod_phase,
)

logger = logging.getLogger(__name__)

# Default port the Spark driver UI binds to; a stable HTTP endpoint to probe.
SPARK_DRIVER_UI_PORT = 4040


def setup_spark_job(
    namespace: str,
    service_account: str,
):
    """Set up a service account for running Spark jobs."""
    logger.info("Setting up Spark job...")
    setup_spark_output = subprocess.check_output(
        f"./tests/integration/setup/setup_spark.sh {service_account} {namespace}",
        shell=True,
        stderr=None,
    ).decode("utf-8")
    logger.info("Spark job setup output:\n%s", setup_spark_output)
    return setup_spark_output


def run_spark_job(
    namespace: str,
    service_account: str,
) -> str:
    """Run a Spark job."""
    logger.info("Executing Spark job...")
    output = subprocess.check_output(
        f"./tests/integration/setup/run_spark_job.sh {service_account} {namespace}",
        shell=True,
        stderr=None,
    ).decode("utf-8")
    return output


def spark_service_account_exists(namespace: str, service_account: str) -> bool:
    """Check whether a `namespace:service-account` Spark service account exists.

    Args:
        namespace: namespace of the Spark service account.
        service_account: name of the Spark service account.
    """
    output = subprocess.check_output(
        ["spark-client.service-account-registry", "list"],
        text=True,
    )
    return f"{namespace}:{service_account}" in output.split()


def run_long_spark_job(
    namespace: str,
    service_account: str,
) -> str:
    """Submit a driver that stays alive until killed via `cleanup_workload_pods`.

    Submits in cluster mode with waitAppCompletion=false, so this returns as soon
    as the job is accepted; the driver and executor pods keep running until the
    driver pod is deleted.
    """
    logger.info("Submitting long-running Spark job...")
    output = subprocess.check_output(
        f"./tests/integration/setup/run_long_spark_job.sh {service_account} {namespace}",
        shell=True,
        stderr=None,
    ).decode("utf-8")
    return output


def wait_for_running_spark_workloads(namespace: str) -> tuple[str, str]:
    """Wait until the driver and one executor pod are Running; return their IPs.

    Returns:
        A tuple of (driver_ip, executor_ip).
    """
    driver_pod = _wait_for_driver_pod(namespace)
    wait_for_pod_phase(driver_pod, namespace=namespace, phase="Running")

    executor_pod = _wait_for_executor_pod(namespace)
    wait_for_pod_phase(executor_pod, namespace=namespace, phase="Running")

    driver_ip = get_pod_ip(driver_pod, namespace=namespace)
    executor_ip = get_pod_ip(executor_pod, namespace=namespace)
    assert driver_ip and executor_ip, "Driver/executor pod has no IP yet"
    return driver_ip, executor_ip


@retry(stop=stop_after_attempt(20), wait=wait_fixed(3), reraise=True)
def _wait_for_driver_pod(namespace: str) -> str:
    """Wait until exactly one driver pod exists and return its name."""
    driver_pods = get_spark_driver_pods(namespace=namespace)
    assert len(driver_pods) == 1, f"Expected exactly one driver pod, found: {driver_pods}"
    return driver_pods[0]


@retry(stop=stop_after_attempt(20), wait=wait_fixed(3), reraise=True)
def _wait_for_executor_pod(namespace: str) -> str:
    """Wait until at least one executor pod exists and return its name."""
    executor_pods = get_spark_executor_pods(namespace=namespace)
    assert executor_pods, "No executor pod scheduled yet"
    return executor_pods[0]


def get_spark_driver_pods(namespace: str | None = None) -> list[str]:
    """Return the names of all Spark driver pods in the given namespace.

    Args:
        namespace: namespace to search in. If None, searches all namespaces.
    """
    return get_pods_by_label(labels={"spark-role": "driver"}, namespace=namespace)


def get_spark_executor_pods(namespace: str | None = None) -> list[str]:
    """Return the names of all Spark executor pods in the given namespace.

    Args:
        namespace: namespace to search in. If None, searches all namespaces.
    """
    return get_pods_by_label(labels={"spark-role": "executor"}, namespace=namespace)


@retry(stop=stop_after_attempt(5), wait=wait_fixed(3), reraise=True)
def assert_spark_job_successful(namespace: str, expected_output: str = "Pi is roughly") -> None:
    """Assert the SparkPi job completed successfully in the given namespace.

    Must be called after `run_spark_job` returns: spark-submit runs with
    waitAppCompletion, so the driver pod is already in a terminal phase. Requires
    cluster mode so that a driver pod exists.

    Args:
        namespace: namespace the driver pod runs in.
        expected_output: substring the driver logs must contain. SparkPi prints
            "Pi is roughly <value>".
    """
    driver_pods = get_spark_driver_pods(namespace=namespace)
    assert len(driver_pods) == 1, f"Expected exactly one driver pod, found: {driver_pods}"
    driver_pod = driver_pods[0]

    phase = get_pod_phase(driver_pod, namespace=namespace)
    assert phase == "Succeeded", f"Driver pod {driver_pod} is in phase {phase}, expected Succeeded"

    logs = get_pod_logs(driver_pod, namespace=namespace)
    assert expected_output in logs, f"'{expected_output}' not found in driver logs of {driver_pod}"


def cleanup_workload_pods(namespace: str, pod_names: list[str] | None = None) -> None:
    """Delete Spark workload (driver/executor) pods.

    Args:
        namespace: namespace the pods live in.
        pod_names: names of the pods to delete. If None, deletes every Spark
            driver and executor pod found in the namespace.
    """
    if pod_names is None:
        pod_names = get_spark_driver_pods(namespace=namespace) + get_spark_executor_pods(
            namespace=namespace
        )

    for pod_name in pod_names:
        delete_pod(pod_name, namespace=namespace)
