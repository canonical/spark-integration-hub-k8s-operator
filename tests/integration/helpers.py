#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

import json
import logging
import os
import subprocess
from tempfile import NamedTemporaryFile
from typing import Dict, TypedDict

import jubilant
import lightkube
from lightkube.resources.core_v1 import Pod

BUCKET_NAME = "test-bucket"

logger = logging.getLogger(__name__)


def sync_s3_credentials(
    juju: jubilant.Juju, unit_name: str, access_key: str, secret_key: str
) -> None:
    params = {"access-key": access_key, "secret-key": secret_key}
    task = juju.run(unit_name, "sync-s3-credentials", params=params)
    assert task.return_code == 0


def run_service_account_registry(*args):
    """Run service_account_registry CLI command with given set of args.

    Returns:
        Tuple: A tuple with the content of stdout, stderr and the return code
            obtained when the command is run.
    """
    command = ["python3", "-m", "spark8t.cli.service_account_registry", *args]
    try:
        output = subprocess.run(command, check=True, capture_output=True)
        return output.stdout.decode(), output.stderr.decode(), output.returncode
    except subprocess.CalledProcessError as e:
        return e.stdout.decode(), e.stderr.decode(), e.returncode


def get_secret_data(namespace: str, secret_name: str):
    """Retrieve secret data for a given namespace and secret."""
    command = ["kubectl", "get", "secret", "-n", namespace, "--output", "json"]
    try:
        output = subprocess.run(command, check=True, capture_output=True)
        # output.stdout.decode(), output.stderr.decode(), output.returncode
        result = output.stdout.decode()
        logger.info(f"Command: {command}")
        logger.info(f"Secrets for namespace: {namespace}")
        logger.info(f"Request secret: {secret_name}")
        logger.info(f"results: {str(result)}")
        secrets = json.loads(result)
        data = {}
        for secret in secrets["items"]:
            name = secret["metadata"]["name"]
            logger.info(f"\t secretName: {name}")
            if name == secret_name:
                data = {}
                if "data" in secret:
                    data = secret["data"]
        return data
    except subprocess.CalledProcessError as e:
        return e.stdout.decode(), e.stderr.decode(), e.returncode


def does_secret_exist(namespace: str, secret_name: str) -> bool:
    """Check secret existence for a given namespace and secret name."""
    command = ["kubectl", "get", "secret", "-n", namespace, "--output", "json"]
    output = subprocess.run(command, check=True, capture_output=True)
    result = output.stdout.decode()
    secrets = json.loads(result)
    for secret in secrets["items"]:
        name = secret["metadata"]["name"]
        logger.info(f"\t secretName: {name}")
        if name == secret_name:
            return True
    return False


def get_address(juju: jubilant.Juju, unit_name: str) -> str:
    status = juju.status()

    app_name, unit_id = unit_name.split("/")
    for name, val in status.apps[app_name].units.items():
        if unit_name == name:
            return val.address
    return ""


def umask_named_temporary_file(*args, **kargs):
    """Return a temporary file descriptor readable by all users."""
    file_desc = NamedTemporaryFile(*args, **kargs)
    mask = os.umask(0o666)
    os.umask(mask)
    os.chmod(file_desc.name, 0o666 & ~mask)
    return file_desc


class ContainerSecurityContext(TypedDict):
    """TypedDict representing Kubernetes container security context settings."""

    runAsUser: int | None  # noqa N815
    runAsGroup: int | None  # noqa N815


def generate_container_securitycontext_map(
    metadata_yaml: dict, juju_user_id: int = 170
) -> dict[str, ContainerSecurityContext]:
    """Generate a mapping of container names to their security context UID/GID settings."""
    c_uid_map = {}
    for k, v in metadata_yaml.get("containers", {}).items():
        c_uid_map[k] = ContainerSecurityContext(
            runAsUser=v["uid"],
            runAsGroup=v["gid"],
        )
    c_uid_map["charm"] = {"runAsUser": juju_user_id, "runAsGroup": juju_user_id}
    return c_uid_map


def get_pod_names(model: str, application_name: str) -> list[str]:
    """Retrieve names of all pods belonging to a specific Juju application."""
    cmd = [
        "kubectl",
        "get",
        "pods",
        f"-n{model}",
        f"-lapp.kubernetes.io/name={application_name}",
        "--no-headers",
        "-o=custom-columns=NAME:.metadata.name",
    ]
    proc = subprocess.run(cmd, stdout=subprocess.PIPE)
    stdout = proc.stdout.decode("utf8")
    return stdout.split()


def assert_security_context(
    lightkube_client: lightkube.Client,
    pod_name: str,
    container_name: str,
    container_securitycontext_map: Dict[str, ContainerSecurityContext],
    model_name: str,
) -> None:
    """Assert that a container's security context matches expected UID/GID settings."""
    containers: list = lightkube_client.get(Pod, pod_name, namespace=model_name).spec.containers
    container = next((c for c in containers if c.name == container_name), None)
    security_context = container.securityContext
    for key, value in container_securitycontext_map.get(container_name).items():
        assert getattr(security_context, key) == value
