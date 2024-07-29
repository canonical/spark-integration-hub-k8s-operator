#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

import json
import logging
from typing import Dict

from juju.unit import Unit
from pytest_operator.plugin import OpsTest

logger = logging.getLogger(__name__)


async def fetch_action_sync_s3_credentials(unit: Unit, access_key: str, secret_key: str) -> Dict:
    """Helper to run an action to sync credentials.

    Args:
        unit: The juju unit on which to run the get-password action for credentials
        access_key: the access_key to access the s3 compatible endpoint
        secret_key: the secret key to access the s3 compatible endpoint
    Returns:
        A dictionary with the server config username and password
    """
    parameters = {"access-key": access_key, "secret-key": secret_key}
    action = await unit.run_action(action_name="sync-s3-credentials", **parameters)
    result = await action.wait()

    return result.results


async def get_juju_secret(ops_test: OpsTest, secret_uri: str) -> Dict[str, str]:
    """Retrieve juju secret."""
    secret_unique_id = secret_uri.split("/")[-1]
    complete_command = f"show-secret {secret_uri} --reveal --format=json"
    _, stdout, _ = await ops_test.juju(*complete_command.split())
    return json.loads(stdout)[secret_unique_id]["content"]["Data"]


async def add_juju_secret(
    ops_test: OpsTest, charm_name: str, secret_label: str, data: Dict[str, str]
) -> str:
    """Add a new juju secret."""
    key_values = " ".join([f"{key}={value}" for key, value in data.items()])
    command = f"add-secret {secret_label} {key_values}"
    _, stdout, _ = await ops_test.juju(*command.split())
    secret_uri = stdout.strip()
    command = f"grant-secret {secret_label} {charm_name}"
    _, stdout, _ = await ops_test.juju(*command.split())
    return secret_uri


async def update_juju_secret(
    ops_test: OpsTest, charm_name: str, secret_label: str, data: Dict[str, str]
) -> str:
    """Update the given juju secret."""
    key_values = " ".join([f"{key}={value}" for key, value in data.items()])
    command = f"update-secret {secret_label} {key_values}"
    retcode, stdout, stderr = await ops_test.juju(*command.split())
    if retcode != 0:
        logger.warning(
            f"Update Juju secret exited with non zero status. \nSTDOUT: {stdout.strip()} \nSTDERR: {stderr.strip()}"
        )
