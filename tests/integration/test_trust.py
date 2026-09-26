#!/usr/bin/env python3
# Copyright 2025 Canonical Limited
# See LICENSE file for licensing details.

import logging
from pathlib import Path

import jubilant
import yaml

from .helpers.integration_hub import (
    deploy_integration_hub_setup,
)
from .types import IntegrationTestsCharms

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]


def test_deploy_integration_hub_with_trust(
    juju: jubilant.Juju, charm_versions: IntegrationTestsCharms, hub_charm: str | Path
) -> None:
    """Test deploying the integration hub charm."""
    deploy_integration_hub_setup(
        juju=juju,
        hub_charm=hub_charm,
        charm_versions=charm_versions,
        trust=True,
    )
    juju.wait(jubilant.all_active)


def test_remove_clusterwide_trust_permissions(juju: jubilant.Juju) -> None:
    """Test removing cluster-wide trust permissions."""
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
