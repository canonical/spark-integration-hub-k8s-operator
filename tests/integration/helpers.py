#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

import json
import logging
import os
import subprocess
from tempfile import NamedTemporaryFile

import jubilant

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
