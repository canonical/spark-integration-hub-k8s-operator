#!/usr/bin/env python3
# Copyright 2025 Canonical Limited
# See LICENSE file for licensing details.

import logging
from pathlib import Path

import jubilant
import yaml

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]


def test_build_and_deploy_hub_charm(juju: jubilant.Juju, deploy_hub_charm: str) -> None:
    juju.wait(lambda status: jubilant.all_active(status, APP_NAME))


def test_remove_clusterwide_trust_permissions(juju: jubilant.Juju) -> None:
    juju.trust(APP_NAME, scope="cluster", remove=True)

    status = juju.wait(
        lambda status: jubilant.any_blocked(
            status,
            APP_NAME,
        ),
    )
    assert (
        status.apps[APP_NAME].app_status.message
        == f"Run `juju trust {APP_NAME} --scope=cluster`. Needed to run."
    )
