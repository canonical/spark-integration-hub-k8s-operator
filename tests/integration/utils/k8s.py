import logging
import subprocess
import uuid
from typing import cast

from lightkube import ApiError, Client
from lightkube.core.client import LabelSelector
from lightkube.resources.core_v1 import Pod
from tenacity import retry, stop_after_attempt, wait_fixed

CURL_IMAGE = "curlimages/curl:8.10.1"

logger = logging.getLogger(__name__)


def get_pods_by_label(labels: dict[str, str], namespace: str | None = None) -> list[str]:
    """Return the names of all pods that carry the given set of labels.

    Args:
        labels: label key/value pairs a pod must all match.
        namespace: namespace to search in. If None, searches all namespaces.
    """
    client = Client()
    try:
        pods = client.list(Pod, labels=cast(LabelSelector, labels), namespace=namespace)
        return [pod.metadata.name for pod in pods if pod.metadata and pod.metadata.name]
    except ApiError as e:
        logger.error(f"Error retrieving pods for labels {labels}: {e}")
        return []


def pod_has_labels(pod_name: str, namespace: str, labels: dict[str, str]) -> bool:
    """Check if a pod has the given set of labels.

    Args:
        pod_name: name of the pod to check.
        namespace: namespace of the pod.
        labels: label key/value pairs the pod must all match.

    Returns:
        True if the pod has all the given labels, False otherwise.
    """
    client = Client()
    try:
        pod = client.get(Pod, name=pod_name, namespace=namespace)
        if not pod.metadata or not pod.metadata.labels:
            return False
        return all(pod.metadata.labels.get(k) == v for k, v in labels.items())
    except ApiError as e:
        logger.error(f"Error retrieving pod {pod_name} in namespace {namespace}: {e}")
        return False


def get_pod_phase(pod_name: str, namespace: str) -> str | None:
    """Return the lifecycle phase of a pod (e.g. "Running", "Succeeded", "Failed").

    A pod that finished successfully reports the phase "Succeeded"; this is what
    `kubectl` displays as "Completed" in its STATUS column.

    Args:
        pod_name: name of the pod to check.
        namespace: namespace of the pod.
    """
    client = Client()
    pod = client.get(Pod, name=pod_name, namespace=namespace)
    return pod.status.phase if pod.status else None


def get_pod_logs(pod_name: str, namespace: str) -> str:
    """Return the full logs of a pod's (only) container.

    Args:
        pod_name: name of the pod to read logs from.
        namespace: namespace of the pod.
    """
    client = Client()
    return "".join(client.log(pod_name, namespace=namespace))


def get_pod_ip(pod_name: str, namespace: str) -> str | None:
    """Return the cluster IP address of a pod, or None if not yet assigned.

    Args:
        pod_name: name of the pod.
        namespace: namespace of the pod.
    """
    client = Client()
    pod = client.get(Pod, name=pod_name, namespace=namespace)
    return pod.status.podIP if pod.status else None


@retry(stop=stop_after_attempt(20), wait=wait_fixed(3), reraise=True)
def wait_for_pod_phase(pod_name: str, namespace: str, phase: str = "Running") -> None:
    """Wait until a pod reaches the given lifecycle phase.

    Args:
        pod_name: name of the pod to wait for.
        namespace: namespace of the pod.
        phase: target phase, e.g. "Running" or "Succeeded".
    """
    current = get_pod_phase(pod_name, namespace=namespace)
    assert current == phase, f"Pod {pod_name} is in phase {current}, waiting for {phase}"


def delete_pod(pod_name: str, namespace: str) -> None:
    """Delete a pod, ignoring the case where it no longer exists.

    Args:
        pod_name: name of the pod to delete.
        namespace: namespace of the pod.
    """
    client = Client()
    try:
        client.delete(Pod, name=pod_name, namespace=namespace)
        logger.info(f"Deleted pod {pod_name} in namespace {namespace}")
    except ApiError as e:
        if e.status.code == 404:
            logger.info(f"Pod {pod_name} in namespace {namespace} already gone")
            return
        logger.error(f"Error deleting pod {pod_name} in namespace {namespace}: {e}")
        raise


def curl_using_pod(
    namespace: str,
    url: str,
    labels: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run a curl command from a temporary pod in the specified namespace."""
    pod_name = f"curl-{uuid.uuid4()}"

    labels_args = []
    if labels:
        # kubectl run --labels accepts a single comma-separated k=v list.
        labels_value = ",".join(f"{key}={value}" for key, value in labels.items())
        labels_args = ["--labels", labels_value]

    return subprocess.run(
        [
            "kubectl",
            "-n",
            namespace,
            "run",
            pod_name,
            "--rm",
            "-i",
            "--quiet",
            "--restart=Never",
            f"--image={CURL_IMAGE}",
            *labels_args,
            "--",
            "curl",
            "-sS",
            "--max-time",
            "10",
            "-o",
            "/dev/null",
            "-w",
            "%{http_code}",
            url,
        ],
        check=False,
        capture_output=True,
        text=True,
    )
