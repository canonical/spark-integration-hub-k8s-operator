#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.


import asyncio
import json
import logging
import subprocess
import urllib.request
import uuid
from pathlib import Path
from time import sleep
from typing import Any, Dict

import boto3
import pytest
import yaml
from botocore.client import Config
from pytest_operator.plugin import OpsTest

from .helpers import add_juju_secret, fetch_action_sync_s3_credentials

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]
BUCKET_NAME = "test-bucket"
CONTAINER_NAME = "test-container"
SECRET_NAME_PREFIX = "integrator-hub-conf-"


@pytest.fixture
def namespace():
    """A temporary K8S namespace gets cleaned up automatically."""
    namespace_name = str(uuid.uuid4())
    create_command = ["kubectl", "create", "namespace", namespace_name]
    subprocess.run(create_command, check=True)
    yield namespace_name
    destroy_command = ["kubectl", "delete", "namespace", namespace_name]
    subprocess.run(destroy_command, check=True)


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


def setup_s3_bucket_for_sch_server(endpoint_url: str, aws_access_key: str, aws_secret_key: str):
    config = Config(connect_timeout=60, retries={"max_attempts": 0})
    session = boto3.session.Session(
        aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key
    )
    s3 = session.client("s3", endpoint_url=endpoint_url, config=config)
    # delete test bucket and its content if it already exist
    buckets = s3.list_buckets()
    for bucket in buckets["Buckets"]:
        bucket_name = bucket["Name"]
        if bucket_name == BUCKET_NAME:
            logger.info(f"Deleting bucket: {bucket_name}")
            objects = s3.list_objects_v2(Bucket=BUCKET_NAME)["Contents"]
            objs = [{"Key": x["Key"]} for x in objects]
            s3.delete_objects(Bucket=BUCKET_NAME, Delete={"Objects": objs})
            s3.delete_bucket(Bucket=BUCKET_NAME)

    logger.info("create bucket in minio")
    for i in range(0, 30):
        try:
            s3.create_bucket(Bucket=BUCKET_NAME)
            break
        except Exception as e:
            if i >= 30:
                logger.error(f"create bucket failed....exiting....\n{str(e)}")
                raise
            else:
                logger.warning(f"create bucket failed....retrying in 10 secs.....\n{str(e)}")
                sleep(10)
                continue

    s3.put_object(Bucket=BUCKET_NAME, Key=("spark-events/"))
    logger.debug(s3.list_buckets())


async def run_action(
    ops_test: OpsTest, action_name: str, params: Dict[str, str], num_unit=0
) -> Any:
    """Use the charm action to start a password rotation."""
    action = await ops_test.model.units.get(f"{APP_NAME}/{num_unit}").run_action(
        action_name, **params
    )
    password = await action.wait()
    return password.results


@pytest.mark.abort_on_fail
async def test_build_and_deploy(ops_test: OpsTest, charm_versions, azure_credentials):
    """Build the charm-under-test and deploy it together with related charms.

    Assert on the unit status before any relations/configurations take place.
    """
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

    logger.info("Building charm")
    # Build and deploy charm from local source folder

    charm = await ops_test.build_charm(".")

    image_version = METADATA["resources"]["integration-hub-image"]["upstream-source"]

    logger.info(f"Image version: {image_version}")

    resources = {"integration-hub-image": image_version}

    logger.info(
        "Deploying Spark Integration hub charm, s3-integrator charm and azure-storage-integrator charm"
    )

    # Deploy the charm and wait for waiting status
    await asyncio.gather(
        ops_test.model.deploy(**charm_versions.s3.deploy_dict()),
        ops_test.model.deploy(**charm_versions.azure_storage.deploy_dict()),
        ops_test.model.deploy(
            charm,
            resources=resources,
            application_name=APP_NAME,
            num_units=1,
            series="jammy",
            trust=True,
        ),
    )

    logger.info("Waiting for s3-integrator and azure-storage-integrator charms to be idle...")
    await ops_test.model.wait_for_idle(
        apps=[
            APP_NAME,
            charm_versions.s3.application_name,
            charm_versions.azure_storage.application_name,
        ],
        timeout=300,
    )

    s3_integrator_unit = ops_test.model.applications[charm_versions.s3.application_name].units[0]

    logger.info("Setting up s3 credentials in s3-integrator charm")

    await fetch_action_sync_s3_credentials(
        s3_integrator_unit, access_key=access_key, secret_key=secret_key
    )
    async with ops_test.fast_forward():
        await ops_test.model.wait_for_idle(
            apps=[charm_versions.s3.application_name], status="active"
        )

    configuration_parameters = {
        "bucket": BUCKET_NAME,
        "path": "spark-events",
        "endpoint": endpoint_url,
    }
    # apply new configuration options
    logger.info("Setting up configuration for s3-integrator charm...")
    await ops_test.model.applications[charm_versions.s3.application_name].set_config(
        configuration_parameters
    )

    logger.info("Adding Juju secret for secret-key config option for azure-storage-integrator")
    credentials_secret_uri = await add_juju_secret(
        ops_test,
        charm_versions.azure_storage.application_name,
        "iamsecret",
        {"secret-key": azure_credentials["secret-key"]},
    )
    logger.info(
        f"Juju secret for secret-key config option for azure-storage-integrator added. Secret URI: {credentials_secret_uri}"
    )

    configuration_parameters = {
        "container": azure_credentials["container"],
        "path": azure_credentials["path"],
        "storage-account": azure_credentials["storage-account"],
        "connection-protocol": azure_credentials["connection-protocol"],
        "credentials": credentials_secret_uri,
    }
    # apply new configuration options
    logger.info("Setting up configuration for azure-storage-integrator charm...")
    await ops_test.model.applications[charm_versions.azure_storage.application_name].set_config(
        configuration_parameters
    )

    logger.info(
        "Waiting for s3-integrator, azure-storage-integrator and integration-hub charm to be idle and active..."
    )
    async with ops_test.fast_forward():
        await ops_test.model.wait_for_idle(
            apps=[
                charm_versions.azure_storage.application_name,
                charm_versions.s3.application_name,
                APP_NAME,
            ],
            status="active",
        )


@pytest.mark.abort_on_fail
async def test_actions(ops_test: OpsTest, charm_versions, namespace, service_account):
    logger.info("Testing actions")
    service_account_name = service_account[0]
    logger.info(f"Service account name: {service_account_name}")
    secret_data = get_secret_data(
        namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )
    logger.info(f"namespace: {namespace} -> secret_data: {secret_data}")
    assert len(secret_data) == 0

    # list config
    res = await run_action(ops_test, "list-config", {})
    assert res["return-code"] == 0
    # wait for active status
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME],
        status="active",
        timeout=1000,
    )
    logger.info(f"List config action result: {res}")
    secret_data = get_secret_data(
        namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )
    logger.info(f"namespace: {namespace} -> secret_data: {secret_data}")
    assert len(secret_data) == 0

    # add new configuration
    res = await run_action(ops_test, "add-config", {"conf": "a=b"})
    assert res["return-code"] == 0
    # wait for active status
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME],
        status="active",
        timeout=1000,
    )
    logger.info(f"add-config action result: {res}")
    sleep(15)
    secret_data = get_secret_data(
        namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )
    logger.info(f"namespace: {namespace} -> secret_data: {secret_data}")
    # check data in secret
    assert "a" in secret_data
    assert len(secret_data) > 0

    # check that previously set configuration option is present
    res = await run_action(ops_test, "list-config", {})
    assert res["return-code"] == 0
    # wait for active status
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME],
        status="active",
        timeout=1000,
    )
    logger.info(f"List-config action result: {res}")
    secret_data = get_secret_data(
        namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )
    logger.info(f"namespace: {namespace} -> secret_data: {secret_data}")
    assert len(secret_data) > 0
    assert "a" in res

    # Remove inserted config
    res = await run_action(ops_test, "remove-config", {"key": "a"})
    assert res["return-code"] == 0
    # wait for active status
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME],
        status="active",
        timeout=1000,
    )
    logger.info(f"Remove-config action result: {res}")
    sleep(15)
    secret_data = get_secret_data(
        namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )
    logger.info(f"namespace: {namespace} -> secret_data: {secret_data}")
    assert len(secret_data) == 0

    # clear config
    res = await run_action(ops_test, "clear-config", {})
    assert res["return-code"] == 0
    # wait for active status
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME],
        status="active",
        timeout=1000,
    )
    sleep(15)
    secret_data = get_secret_data(
        namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )
    logger.info(f"namespace: {namespace} -> secret_data: {secret_data}")
    logger.info(f"Clear-config action result: {res}")
    assert len(secret_data) == 0


@pytest.mark.abort_on_fail
async def test_relation_to_s3(ops_test: OpsTest, charm_versions, namespace, service_account):

    logger.info("Relating spark integration hub charm with s3-integrator charm")
    service_account_name = service_account[0]
    secret_data = get_secret_data(
        namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )
    logger.info(f"namespace: {namespace} -> secret_data: {secret_data}")
    assert len(secret_data) == 0

    await ops_test.model.add_relation(charm_versions.s3.application_name, APP_NAME)

    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, charm_versions.s3.application_name], timeout=1000
    )

    # wait for active status
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME],
        status="active",
        timeout=1000,
    )

    # wait for secret update
    logger.info("Wait for secret update.")
    sleep(15)

    secret_data = get_secret_data(
        namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )
    logger.info(f"namespace: {namespace} -> secret_data: {secret_data}")
    assert len(secret_data) > 0
    assert "spark.hadoop.fs.s3a.access.key" in secret_data


@pytest.mark.abort_on_fail
async def test_add_new_service_account_with_s3(ops_test: OpsTest, namespace, service_account):
    service_account_name = service_account[0]

    # wait for the update of secrets
    sleep(20)
    # check secret

    secret_data = get_secret_data(
        namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )
    assert len(secret_data) > 0
    assert "spark.hadoop.fs.s3a.access.key" in secret_data


# @pytest.mark.abort_on_fail
# async def test_add_removal_s3_relation(
#     ops_test: OpsTest, namespace, service_account, charm_versions
# ):
#     service_account_name = service_account[0]
#     # wait for the update of secrets
#     sleep(15)
#     # check secret
#
#     secret_data = get_secret_data(
#         namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
#     )
#     assert len(secret_data) > 0
#     assert "spark.hadoop.fs.s3a.access.key" in secret_data
#
#     await ops_test.model.applications[APP_NAME].remove_relation(
#         f"{APP_NAME}:s3-credentials", f"{charm_versions.s3.application_name}:s3-credentials"
#     )
#
#     await ops_test.model.wait_for_idle(
#         apps=[APP_NAME, charm_versions.s3.application_name],
#         status="active",
#         timeout=300,
#         idle_period=30,
#     )
#
#     secret_data = get_secret_data(
#         namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
#     )
#     assert len(secret_data) == 0
#
#     await ops_test.model.add_relation(charm_versions.s3.application_name, APP_NAME)
#
#     await ops_test.model.wait_for_idle(
#         apps=[APP_NAME, charm_versions.s3.application_name], timeout=1000
#     )
#
#     # wait for active status
#     await ops_test.model.wait_for_idle(
#         apps=[APP_NAME],
#         status="active",
#         timeout=1000,
#     )
#
#     # wait for secret update
#     logger.info("Wait for secret update.")
#     sleep(15)
#
#     secret_data = get_secret_data(
#         namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
#     )
#     logger.info(f"namespace: {namespace} -> secret_data: {secret_data}")
#     assert len(secret_data) > 0
#     assert "spark.hadoop.fs.s3a.access.key" in secret_data


@pytest.mark.abort_on_fail
async def test_relation_to_both_s3_and_azure_storage_at_same_time(
    ops_test: OpsTest, charm_versions
):

    logger.info(
        "Relating spark integration hub charm with azure-storage-integrator along with existing relation with s3-integrator charm"
    )

    await ops_test.model.add_relation(charm_versions.azure_storage.application_name, APP_NAME)

    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, charm_versions.azure_storage.application_name], timeout=1000
    )

    # wait for blocked status
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME],
        status="blocked",
        timeout=1000,
    )

    # Remove relation with both S3 and Azure Storage
    await ops_test.model.applications[APP_NAME].remove_relation(
        f"{APP_NAME}:s3-credentials", f"{charm_versions.s3.application_name}:s3-credentials"
    )
    await ops_test.model.applications[APP_NAME].remove_relation(
        f"{APP_NAME}:azure-credentials",
        f"{charm_versions.azure_storage.application_name}:azure-credentials",
    )

    # wait for active status
    await ops_test.model.wait_for_idle(
        apps=[
            APP_NAME,
            charm_versions.azure_storage.application_name,
            charm_versions.s3.application_name,
        ],
        status="active",
        timeout=1000,
    )


@pytest.mark.abort_on_fail
async def test_relation_to_azure_storage(
    ops_test: OpsTest, charm_versions, namespace, service_account, azure_credentials
):

    logger.info("Relating spark integration hub charm with azure-storage-integrator charm")
    service_account_name = service_account[0]
    secret_data = get_secret_data(
        namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )
    logger.info(f"namespace: {namespace} -> secret_data: {secret_data}")
    assert len(secret_data) == 0

    await ops_test.model.add_relation(charm_versions.azure_storage.application_name, APP_NAME)

    # wait for active status
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, charm_versions.azure_storage.application_name],
        status="active",
        timeout=1000,
    )

    # wait for secret update
    logger.info("Wait for secret update.")
    sleep(15)

    secret_data = get_secret_data(
        namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )
    logger.info(f"namespace: {namespace} -> secret_data: {secret_data}")
    assert len(secret_data) > 0
    assert (
        f"spark.hadoop.fs.azure.account.key.{azure_credentials['storage-account']}.dfs.core.windows.net"
        in secret_data
    )


@pytest.mark.abort_on_fail
async def test_add_new_service_account_with_azure_storage(
    ops_test: OpsTest, namespace, service_account, azure_credentials
):
    service_account_name = service_account[0]

    # wait for the update of secrets
    sleep(15)
    # check secret

    secret_data = get_secret_data(
        namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )
    assert len(secret_data) > 0
    assert (
        f"spark.hadoop.fs.azure.account.key.{azure_credentials['storage-account']}.dfs.core.windows.net"
        in secret_data
    )


@pytest.mark.abort_on_fail
async def test_add_removal_azure_storage_relation(
    ops_test: OpsTest, namespace, service_account, charm_versions, azure_credentials
):
    service_account_name = service_account[0]
    # wait for the update of secrets
    sleep(15)
    # check secret

    secret_data = get_secret_data(
        namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )
    assert len(secret_data) > 0
    assert (
        f"spark.hadoop.fs.azure.account.key.{azure_credentials['storage-account']}.dfs.core.windows.net"
        in secret_data
    )

    await ops_test.model.applications[APP_NAME].remove_relation(
        f"{APP_NAME}:azure-credentials",
        f"{charm_versions.azure_storage.application_name}:azure-credentials",
    )

    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, charm_versions.azure_storage.application_name],
        status="active",
        timeout=300,
        idle_period=30,
    )

    secret_data = get_secret_data(
        namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )
    assert len(secret_data) == 0

    await ops_test.model.add_relation(charm_versions.azure_storage.application_name, APP_NAME)

    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, charm_versions.azure_storage.application_name], timeout=1000
    )

    # wait for active status
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, charm_versions.azure_storage.application_name],
        status="active",
        timeout=1000,
    )

    # wait for secret update
    logger.info("Wait for secret update.")
    sleep(15)

    secret_data = get_secret_data(
        namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )
    logger.info(f"namespace: {namespace} -> secret_data: {secret_data}")
    assert len(secret_data) > 0
    assert (
        f"spark.hadoop.fs.azure.account.key.{azure_credentials['storage-account']}.dfs.core.windows.net"
        in secret_data
    )


@pytest.mark.abort_on_fail
async def test_relation_to_pushgateway(
    ops_test: OpsTest, charm_versions, namespace, service_account
):

    logger.info("Relating spark integration hub charm with s3-integrator charm")
    service_account_name = service_account[0]
    # namespace= ops_test.model_name
    logger.info(f"Test with namespace: {namespace}")
    await ops_test.model.deploy(**charm_versions.pushgateway.deploy_dict())

    await ops_test.model.wait_for_idle(
        apps=[charm_versions.pushgateway.application_name], timeout=1000, status="active"
    )

    await ops_test.model.add_relation(charm_versions.pushgateway.application_name, APP_NAME)

    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, charm_versions.pushgateway.application_name],
        status="active",
        timeout=1000,
    )

    sleep(15)

    secret_data = get_secret_data(
        namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )
    logger.info(f"namespace: {namespace} -> secret_data: {secret_data}")

    conf_prop = False
    for key in secret_data.keys():
        if "spark.metrics.conf" in key:
            conf_prop = True
            break
    assert conf_prop

    status = await ops_test.model.get_status()
    address = status["applications"][charm_versions.pushgateway.application_name]["units"][
        f"{charm_versions.pushgateway.application_name}/0"
    ]["address"]

    metrics = json.loads(urllib.request.urlopen(f"http://{address}:9091/api/v1/metrics").read())

    assert len(metrics["data"]) == 0

    setup_spark_output = subprocess.check_output(
        f"./tests/integration/setup/setup_spark.sh {service_account_name} {namespace}",
        shell=True,
        stderr=None,
    ).decode("utf-8")

    logger.info(f"Setup spark output:\n{setup_spark_output}")

    logger.info("Executing Spark job")

    run_spark_output = subprocess.check_output(
        f"./tests/integration/setup/run_spark_job.sh {service_account_name} {namespace}",
        shell=True,
        stderr=None,
    ).decode("utf-8")

    logger.info(f"Run spark output:\n{run_spark_output}")

    logger.info("Verifying metrics is present in the pushgateway has")

    metrics = json.loads(urllib.request.urlopen(f"http://{address}:9091/api/v1/metrics").read())

    logger.info(f"Metrics: {metrics}")

    assert len(metrics["data"]) > 0

    await ops_test.model.applications[APP_NAME].remove_relation(
        f"{APP_NAME}:cos", f"{charm_versions.pushgateway.application_name}:push-endpoint"
    )

    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, charm_versions.pushgateway.application_name],
        status="active",
        timeout=300,
        idle_period=30,
    )

    secret_data = get_secret_data(
        namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )

    for key in secret_data.keys():
        if "spark.metrics.conf" in key:
            assert False


@pytest.mark.abort_on_fail
async def test_remove_application(ops_test: OpsTest, namespace, service_account, charm_versions):
    service_account_name = service_account[0]

    # wait for the update of secres
    sleep(15)
    # check secret

    secret_data = get_secret_data(
        namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )
    assert len(secret_data) > 0
    assert "spark.hadoop.fs.s3a.access.key" in secret_data

    logger.info(f"Remove {APP_NAME}")
    await ops_test.model.remove_application(APP_NAME, block_until_done=True, timeout=600)

    await ops_test.model.wait_for_idle(
        apps=[charm_versions.s3.application_name], status="active", timeout=300
    )

    secret_data = get_secret_data(
        namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )
    logger.info(f"secret data: {secret_data}")
    assert len(secret_data) == 0
