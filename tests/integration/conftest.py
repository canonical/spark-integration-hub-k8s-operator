#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.
import logging
import os
import shutil
import subprocess
import uuid
from pathlib import Path
from platform import machine
from typing import Iterable

import boto3.session
import jubilant
import pytest
import yaml
from botocore.client import Config
from dotenv import load_dotenv

from .helpers import run_service_account_registry
from .types import AzureInfo, CharmVersion, IntegrationTestsCharms, S3Info

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]
BUCKET_NAME = "test-bucket"
PATH_NAME = "spark-events"

load_dotenv()
logger = logging.getLogger(__name__)
logging.getLogger("jubilant.wait").setLevel(logging.WARNING)


def pytest_addoption(parser):
    parser.addoption(
        "--keep-models",
        action="store_true",
        default=False,
        help="keep temporarily-created models",
    )
    parser.addoption(
        "--model",
        action="store",
        help="Juju model to use; if not provided, a new model "
        "will be created for each test which requires one",
    )


@pytest.fixture
def charm_versions() -> IntegrationTestsCharms:
    return IntegrationTestsCharms(
        s3=CharmVersion(name="s3-integrator", channel="2/edge", base="ubuntu@24.04", alias="s3"),
        azure_storage=CharmVersion(
            name="azure-storage-integrator",
            channel="1/edge",
            base="ubuntu@22.04",
            alias="azure-storage",
            revision=89,
        ),
        pushgateway=CharmVersion(
            name="prometheus-pushgateway-k8s",
            channel="1/stable",
            base="ubuntu@22.04",
            alias="pushgateway",
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
def azure_credentials() -> AzureInfo:
    return {
        "container": "test-container",
        "path": "spark-events",
        "storage-account": "test-storage-account",
        "connection-protocol": "abfss",
        "secret-key": "i-am-secret",
    }


@pytest.fixture(scope="module")
def s3_credentials(request: pytest.FixtureRequest) -> Iterable[S3Info]:
    keep_models = bool(request.config.getoption("--keep-models"))
    access_key = os.environ["S3_ACCESS_KEY"]
    secret_key = os.environ["S3_SECRET_KEY"]
    endpoint_url = os.environ["S3_SERVER_URL"]

    session = boto3.session.Session(aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    s3 = session.resource(
        service_name="s3",
        endpoint_url=endpoint_url,
        verify=False,
        config=Config(
            connect_timeout=60,
            retries={"max_attempts": 4},
            request_checksum_calculation="when_supported",
            response_checksum_validation="when_supported",
        ),
    )
    test_bucket = s3.Bucket(BUCKET_NAME)

    # Delete test bucket if it exists
    if test_bucket in s3.buckets.all():
        logger.info(f"The bucket {BUCKET_NAME} already exists. Deleting it...")
        for obj in test_bucket.objects.all():
            # We need to iterate over keys because delete_objects (plural) has mandatory checksum
            obj.delete()
        test_bucket.delete()

    # Create the test bucket
    s3.create_bucket(Bucket=BUCKET_NAME)
    logger.info(f"Created bucket: {BUCKET_NAME}")
    test_bucket.put_object(Key=os.path.join(PATH_NAME, "touch"))
    yield {
        "endpoint": str(endpoint_url),
        "access_key": str(access_key),
        "secret_key": str(secret_key),
        "bucket": BUCKET_NAME,
        "path": PATH_NAME,
        "ca_bundle_path": os.environ.get("S3_CA_BUNDLE_PATH", ""),
    }

    if not keep_models:
        logger.info("Tearing down test bucket...")
        for obj in test_bucket.objects.all():
            # We need to iterate over keys because delete_objects (plural) has mandatory checksum
            obj.delete()

        test_bucket.delete()


@pytest.fixture()
def service_account(namespace) -> tuple[str, str]:
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
def platform() -> str:
    """Fixture to provide the platform architecture for testing."""
    platforms = {
        "x86_64": "amd64",
        "aarch64": "arm64",
    }
    return platforms.get(machine(), "amd64")


@pytest.fixture(scope="module")
def hub_charm(platform: str) -> Path:
    """Path to the packed integration hub charm."""
    if not (path := next(iter(Path.cwd().glob(f"*-{platform}.charm")), None)):
        raise FileNotFoundError("Could not find packed integration hub charm.")

    return path


@pytest.fixture(scope="module")
def test_charm(platform: str) -> Path:
    if not (
        path := next(
            iter((Path.cwd() / "tests/integration/app-charm").glob(f"*-{platform}.charm")), None
        )
    ):
        raise FileNotFoundError("Could not find packed test charm.")

    return path


@pytest.fixture(scope="module")
def juju(request: pytest.FixtureRequest, platform: str):
    keep_models = bool(request.config.getoption("--keep-models"))
    model = request.config.getoption("--model")
    model_name = str(model)

    if model is None:
        with jubilant.temp_model(keep=keep_models) as juju:
            juju.wait_timeout = 10 * 60
            juju.cli("set-model-constraints", f"arch={platform}")
            yield juju

    else:
        juju = jubilant.Juju()
        juju.model = model_name
        try:
            juju.status()
        except jubilant.CLIError:
            juju.add_model(model_name)

        juju.wait_timeout = 10 * 60
        juju.cli("set-model-constraints", f"arch={platform}")
        yield juju

    if model is not None and not keep_models:
        juju.destroy_model(model_name, destroy_storage=True, force=True)


@pytest.fixture
def deploy_hub_charm(juju: jubilant.Juju, hub_charm: Path) -> str:
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
def deploy_s3_integrator_charm(juju: jubilant.Juju, charm_versions, s3_credentials: S3Info) -> str:
    juju.deploy(**charm_versions.s3.deploy_dict())
    juju.wait(jubilant.all_agents_idle)

    endpoint_url = s3_credentials["endpoint"]
    access_key = s3_credentials["access_key"]
    secret_key = s3_credentials["secret_key"]

    creds_secret_uri = juju.add_secret(
        "s3-creds", {"access-key": access_key, "secret-key": secret_key}
    )
    juju.grant_secret(creds_secret_uri, charm_versions.s3.application_name)
    juju.config(
        charm_versions.s3.application_name,
        {
            "bucket": BUCKET_NAME,
            "path": "spark-events",
            "endpoint": endpoint_url,
            "credentials": creds_secret_uri,
        },
    )
    juju.wait(lambda status: jubilant.all_active(status, charm_versions.s3.application_name))
    return charm_versions.s3.application_name
