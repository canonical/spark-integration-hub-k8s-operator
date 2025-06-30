#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.
import logging
import shutil
import subprocess
import uuid
from pathlib import Path
from typing import Optional

import jubilant
import pytest
import yaml
from pydantic import BaseModel

from .helpers import BUCKET_NAME, run_service_account_registry, setup_s3_bucket_for_sch_server

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]


def pytest_addoption(parser):
    parser.addoption(
        "--keep-models",
        action="store_true",
        default=False,
        help="keep temporarily-created models",
    )


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
    base: str
    num_units: int = 1
    alias: Optional[str] = None
    trust: bool = False

    @property
    def application_name(self) -> str:
        return self.alias or self.name

    def deploy_dict(self):
        return {
            "charm": self.name,
            "channel": self.channel,
            "base": self.base,
            "num_units": self.num_units,
            "app": self.application_name,
            "trust": self.trust,
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
            **{"name": "s3-integrator", "channel": "edge", "base": "ubuntu@22.04", "alias": "s3"}
        ),
        azure_storage=CharmVersion(
            **{
                "name": "azure-storage-integrator",
                "channel": "edge",
                "base": "ubuntu@22.04",
                "alias": "azure-storage",
            }
        ),
        pushgateway=CharmVersion(
            **{
                "name": "prometheus-pushgateway-k8s",
                "channel": "1/stable",
                "base": "ubuntu@22.04",
                "alias": "pushgateway",
            }
        ),
        grafana_agent=CharmVersion(
            name="grafana-agent-k8s", channel="1/stable", base="ubuntu@22.04"
        ),
    )


@pytest.fixture(scope="module")
def namespace():
    """A temporary K8S namespace gets cleaned up automatically."""
    namespace_name = str(uuid.uuid4())
    create_command = ["kubectl", "create", "namespace", namespace_name]
    subprocess.run(create_command, check=True)
    yield namespace_name
    destroy_command = ["kubectl", "delete", "namespace", namespace_name]
    subprocess.run(destroy_command, check=True)


@pytest.fixture(scope="module", autouse=True)
def copy_hub_library_into_charm():
    """Copy the data_interfaces library to the different charm folder."""
    library_path = "lib/charms/spark_integration_hub_k8s/v0/spark_service_account.py"
    install_path = "tests/integration/app-charm/" + library_path
    shutil.copyfile(f"{library_path}", install_path)


@pytest.fixture(scope="module", autouse=True)
def copy_data_interfaces_library_into_charm():
    """Copy the data_interfaces library to the different charm folder."""
    library_path = "lib/charms/data_platform_libs/v0/data_interfaces.py"
    install_path = "tests/integration/app-charm/" + library_path
    shutil.copyfile(f"{library_path}", install_path)


@pytest.fixture(scope="module")
def azure_credentials():
    return {
        "container": "test-container",
        "path": "spark-events",
        "storage-account": "test-storage-account",
        "connection-protocol": "abfss",
        "secret-key": "i-am-secret",
    }


@pytest.fixture(scope="module")
def s3_credentials():
    logger.info("Setting up minio.....")
    setup_minio_output = (
        subprocess.check_output(
            "./tests/integration/setup/setup_minio.sh | tail -n 1", shell=True, stderr=None
        )
        .decode("utf-8")
        .strip()
    )

    logger.info(f"Minio output:\n{setup_minio_output}")

    s3_params = setup_minio_output.strip().split(",")
    endpoint_url = s3_params[0]
    access_key = s3_params[1]
    secret_key = s3_params[2]

    logger.info(
        f"Setting up s3 bucket with endpoint_url={endpoint_url}, access_key={access_key}, secret_key={secret_key}"
    )
    setup_s3_bucket_for_sch_server(endpoint_url, access_key, secret_key)
    logger.info("Bucket setup complete")
    return {"endpoint": endpoint_url, "access-key": access_key, "secret-key": secret_key}


@pytest.fixture()
def service_account(namespace):
    """A fixture that creates a service account that has the permission to run spark jobs."""
    username = str(uuid.uuid4())

    stdout, stderr, retcode = run_service_account_registry(
        "create",
        "--username",
        username,
        "--namespace",
        namespace,
    )
    if retcode != 0:
        logger.error(f"Error in creation of service account, stdout={stdout}, stderr={stderr}")
    logger.info(f"Service account: {username} created in namespace: {namespace}")
    return username, namespace


@pytest.fixture(scope="module")
def hub_charm() -> Path:
    """Path to the packed integration hub charm."""
    if not (path := next(iter(Path.cwd().glob("*.charm")), None)):
        raise FileNotFoundError("Could not find packed integration hub charm.")

    return path


@pytest.fixture(scope="module")
def test_charm() -> Path:
    if not (
        path := next(iter((Path.cwd() / "tests/integration/app-charm").glob("*.charm")), None)
    ):
        raise FileNotFoundError("Could not find packed test charm.")

    return path


@pytest.fixture(scope="module")
def juju(request: pytest.FixtureRequest):
    keep_models = bool(request.config.getoption("--keep-models"))

    with jubilant.temp_model(keep=keep_models) as juju:
        juju.wait_timeout = 10 * 60

        yield juju  # run the test

        if request.session.testsfailed:
            log = juju.debug_log(limit=30)
            print(log, end="")

        status = juju.cli("status")
        debug_log = juju.debug_log(limit=1000)
        logger.info(debug_log)
        logger.info(status)


@pytest.fixture
def deploy_hub_charm(juju: jubilant.Juju, hub_charm: Path) -> None:
    image_version = METADATA["resources"]["integration-hub-image"]["upstream-source"]
    logger.info(f"Image version: {image_version}")

    resources = {"integration-hub-image": image_version}
    logger.info(
        "Deploying Spark Integration hub charm, s3-integrator charm and azure-storage-integrator charm"
    )
    juju.deploy(
        hub_charm, app=APP_NAME, resources=resources, num_units=1, base="ubuntu@22.04", trust=True
    )
    juju.wait(lambda status: jubilant.all_active(status, APP_NAME))
    return APP_NAME


@pytest.fixture
def deploy_s3_integrator_charm(
    juju: jubilant.Juju, charm_versions, s3_credentials: dict[str, str]
) -> None:
    juju.deploy(**charm_versions.s3.deploy_dict())
    juju.wait(jubilant.all_agents_idle)

    endpoint_url = s3_credentials["endpoint"]
    access_key = s3_credentials["access-key"]
    secret_key = s3_credentials["secret-key"]
    juju.config(
        charm_versions.s3.application_name,
        {
            "bucket": BUCKET_NAME,
            "path": "spark-events",
            "endpoint": endpoint_url,
        },
    )
    task = juju.run(
        f"{charm_versions.s3.application_name}/0",
        "sync-s3-credentials",
        params={"secret-key": secret_key, "access-key": access_key},
    )
    assert task.return_code == 0
    juju.wait(lambda status: jubilant.all_active(status, charm_versions.s3.application_name))
    return charm_versions.s3.application_name
