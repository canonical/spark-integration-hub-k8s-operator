#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.
import logging
import shutil
import subprocess
import uuid
from typing import Optional

import pytest
from pydantic import BaseModel
from pytest_operator.plugin import OpsTest

from .helpers import run_service_account_registry

logger = logging.getLogger(__name__)


class CharmVersion(BaseModel):
    """Identifiable for specifying a version of a charm to be deployed.

    Attrs:
        name: str, representing the charm to be deployed
        channel: str, representing the channel to be used
        series: str, representing the series of the system for the container where the charm
            is deployed to
        num_units: int, number of units for the deployment
    """

    name: str
    channel: str
    series: str
    num_units: int = 1
    alias: Optional[str] = None

    @property
    def application_name(self) -> str:
        return self.alias or self.name

    def deploy_dict(self):
        return {
            "entity_url": self.name,
            "channel": self.channel,
            "series": self.series,
            "num_units": self.num_units,
            "application_name": self.application_name,
        }


class IntegrationTestsCharms(BaseModel):
    s3: CharmVersion
    pushgateway: CharmVersion
    azure_storage: CharmVersion
    grafana_agent: CharmVersion


@pytest.fixture
def charm_versions() -> IntegrationTestsCharms:
    return IntegrationTestsCharms(
        s3=CharmVersion(
            **{"name": "s3-integrator", "channel": "edge", "series": "jammy", "alias": "s3"}
        ),
        azure_storage=CharmVersion(
            **{
                "name": "azure-storage-integrator",
                "channel": "edge",
                "series": "jammy",
                "alias": "azure-storage",
            }
        ),
        pushgateway=CharmVersion(
            **{
                "name": "prometheus-pushgateway-k8s",
                "channel": "stable",
                "series": "jammy",
                "alias": "pushgateway",
            }
        ),
        grafana_agent=CharmVersion(
            name="grafana-agent-k8s", channel="latest/stable", series="jammy"
        ),
    )


@pytest.fixture
def namespace():
    """A temporary K8S namespace gets cleaned up automatically."""
    namespace_name = str(uuid.uuid4())
    create_command = ["kubectl", "create", "namespace", namespace_name]
    subprocess.run(create_command, check=True)
    yield namespace_name
    destroy_command = ["kubectl", "delete", "namespace", namespace_name]
    subprocess.run(destroy_command, check=True)


@pytest.fixture(scope="module", autouse=True)
def copy_hub_library_into_charm(ops_test: OpsTest):
    """Copy the data_interfaces library to the different charm folder."""
    library_path = "src/relations/spark_sa.py"
    install_path = "tests/integration/app-charm/" + library_path
    shutil.copyfile(f"{library_path}", install_path)


@pytest.fixture(scope="module", autouse=True)
def copy_data_interfaces_library_into_charm(ops_test: OpsTest):
    """Copy the data_interfaces library to the different charm folder."""
    library_path = "lib/charms/data_platform_libs/v0/data_interfaces.py"
    install_path = "tests/integration/app-charm/" + library_path
    shutil.copyfile(f"{library_path}", install_path)


@pytest.fixture(scope="module")
def azure_credentials(ops_test: OpsTest):
    return {
        "container": "test-container",
        "path": "spark-events",
        "storage-account": "test-storage-account",
        "connection-protocol": "abfss",
        "secret-key": "i-am-secret",
    }


@pytest.fixture()
def service_account(namespace):
    """A temporary service account that gets cleaned up automatically."""
    username = str(uuid.uuid4())

    run_service_account_registry(
        "create",
        "--username",
        username,
        "--namespace",
        namespace,
    )
    logger.info(f"Service account: {username} created in namespace: {namespace}")
    return username, namespace
