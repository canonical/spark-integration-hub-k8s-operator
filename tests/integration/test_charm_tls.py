#!/usr/bin/env python3
# Copyright 2026 Canonical Limited
# See LICENSE file for licensing details.


import hashlib
import json
import logging
import os
import shutil
import socket
import subprocess
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterable

import boto3
import botocore
import jubilant
import pytest
import yaml
from botocore.client import Config

from .helpers import does_secret_exist, get_secret_data
from .types import IntegrationTestsCharms

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]
CONTAINER_NAME = "test-container"
SECRET_NAME_PREFIX = "integrator-hub-conf-"

BUCKET_NAME = "test-bucket"
PATH_NAME = "spark-events"
DUMMY_APP_NAME = "app"

REL_NAME_A = "spark-account-a"
REL_NAME_B = "spark-account-b"

MICROCEPH_REVISION = 1169


@contextmanager
def local_tmp_folder(name: str = "tmp"):
    if (tmp_folder := Path.cwd() / name).exists():
        shutil.rmtree(tmp_folder)
    tmp_folder.mkdir()

    yield tmp_folder

    shutil.rmtree(tmp_folder)


@pytest.fixture(scope="module")
def context() -> dict[Any, Any]:
    """A common data store read+writeable by all tests."""
    context = {}
    return context


@pytest.fixture(scope="module")
def host_ip() -> str:
    """The IP address of the host running these tests."""
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.connect(("1.1.1.1", 80))
        return s.getsockname()[0]


@pytest.fixture(scope="module")
def certs_path() -> Iterable[Path]:
    """A temporary directory to store certificates and keys."""
    with local_tmp_folder("temp-certs") as tmp_folder:
        yield tmp_folder


@pytest.fixture(scope="module")
def microceph_credentials(host_ip: str, certs_path: Path) -> Iterable[dict[str, str]]:
    """Install, bootstrap and configure Microceph and return credentials."""
    logger.info("Setting up TLS certificates")
    subprocess.run(["openssl", "genrsa", "-out", str(certs_path / "ca.key"), "2048"], check=True)
    subprocess.run(
        [
            "openssl",
            "req",
            "-x509",
            "-new",
            "-nodes",
            "-key",
            str(certs_path / "ca.key"),
            "-days",
            "1024",
            "-out",
            str(certs_path / "ca.crt"),
            "-outform",
            "PEM",
            "-subj",
            f"/C=US/ST=Denial/L=Springfield/O=Dis/CN={host_ip}",
        ],
        check=True,
    )
    subprocess.run(
        ["openssl", "genrsa", "-out", str(certs_path / "server.key"), "2048"],
        check=True,
    )
    subprocess.run(
        [
            "openssl",
            "req",
            "-new",
            "-key",
            str(certs_path / "server.key"),
            "-out",
            str(certs_path / "server.csr"),
            "-subj",
            f"/C=US/ST=Denial/L=Springfield/O=Dis/CN={host_ip}",
        ],
        check=True,
    )

    with open(certs_path / "extfile.cnf", "w") as extfile:
        extfile.write(f"subjectAltName = DNS:{host_ip}, IP:{host_ip}")

    subprocess.run(
        [
            "openssl",
            "x509",
            "-req",
            "-in",
            str(certs_path / "server.csr"),
            "-CA",
            str(certs_path / "ca.crt"),
            "-CAkey",
            str(certs_path / "ca.key"),
            "-CAcreateserial",
            "-out",
            str(certs_path / "server.crt"),
            "-days",
            "365",
            "-extfile",
            str(certs_path / "extfile.cnf"),
        ],
    )

    try:
        subprocess.run(
            ["sudo", "snap", "remove", "microceph", "--purge"],
            check=True,
            stderr=subprocess.PIPE,
        )
    except subprocess.CalledProcessError as ex:
        logger.error(ex.stderr.decode())

    logger.info("Setting up microceph")
    subprocess.run(
        ["sudo", "snap", "install", "microceph"],
        check=True,
    )
    try:
        subprocess.run(
            ["sudo", "microceph", "cluster", "bootstrap"],
            check=True,
            stderr=subprocess.PIPE,
        )
    except subprocess.CalledProcessError as ex:
        logger.error(ex.stderr.decode())

    subprocess.run(
        ["sudo", "microceph", "disk", "add", "loop,1G,3"],
        check=True,
    )
    server_crt_base64 = subprocess.run(
        ["sudo", "base64", "-w0", str(certs_path / "server.crt")],
        check=True,
        text=True,
        capture_output=True,
    ).stdout.strip()
    server_key_base64 = subprocess.run(
        ["sudo", "base64", "-w0", str(certs_path / "server.key")],
        check=True,
        text=True,
        capture_output=True,
    ).stdout.strip()
    logger.info("Enabling rest gateway")
    subprocess.run(
        [
            "sudo",
            "microceph",
            "enable",
            "rgw",
            "--ssl-certificate",
            server_crt_base64,
            "--ssl-private-key",
            server_key_base64,
        ],
        check=True,
    )

    output = subprocess.run(
        [
            "sudo",
            "microceph.radosgw-admin",
            "user",
            "create",
            "--uid",
            "test",
            "--display-name",
            "test",
        ],
        capture_output=True,
        check=True,
        encoding="utf-8",
    ).stdout
    key = json.loads(output)["keys"][0]
    key_id = key["access_key"]
    secret_key = key["secret_key"]
    logger.info("Creating microceph bucket")
    for attempt in range(3):
        try:
            boto3.client(
                "s3",
                endpoint_url=f"https://{host_ip}",
                aws_access_key_id=key_id,
                aws_secret_access_key=secret_key,
                verify=certs_path / "ca.crt",
            ).create_bucket(Bucket="test-bucket")
        except botocore.exceptions.EndpointConnectionError:
            if attempt == 2:
                raise
            # microceph is not ready yet
            logger.info("Unable to connect to microceph via S3. Retrying")
            time.sleep(1)
        else:
            break
    logger.info("Microceph was setup successfully...")

    ca_crt_base64 = subprocess.run(
        ["sudo", "base64", "-w0", str(certs_path / "ca.crt")],
        check=True,
        text=True,
        capture_output=True,
    ).stdout.strip()

    yield {
        "endpoint": f"https://{host_ip}",
        "access-key": key_id,
        "secret-key": secret_key,
        "tls-ca": ca_crt_base64,
    }

    subprocess.run(["sudo", "snap", "remove", "microceph", "--purge"], check=True)


def configure_s3_bucket(s3_endpoint: str, s3_access_key: str, s3_secret_key: str) -> None:
    """Create a path in the S3 bucket to be used for testing."""
    session = boto3.session.Session(
        aws_access_key_id=s3_access_key, aws_secret_access_key=s3_secret_key
    )
    s3 = session.resource(
        service_name="s3",
        endpoint_url=s3_endpoint,
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


def get_truststore_secret_name(model_name: str, app_name: str) -> str:
    """The default truststore secret name, incorporating a hash of model and app name."""
    suffix = hashlib.sha256(f"{model_name}|{app_name}".encode()).hexdigest()[:8]
    return f"{SECRET_NAME_PREFIX}truststore-{suffix}"


def test_build_and_deploy_hub_charm(juju: jubilant.Juju, deploy_hub_charm: str) -> None:
    juju.wait(lambda status: jubilant.all_active(status, APP_NAME))


def test_deploy_s3_integrator(juju: jubilant.Juju, charm_versions, microceph_credentials) -> None:
    """Deploy an extra instance of s3-integrator, this time for creating Postgresql (metastore) backup."""
    juju.deploy("s3-integrator", app=charm_versions.s3.application_name, channel="edge")
    juju.wait(lambda status: jubilant.all_blocked(status, charm_versions.s3.application_name))

    s3_endpoint = microceph_credentials["endpoint"]
    s3_access_key = microceph_credentials["access-key"]
    s3_secret_key = microceph_credentials["secret-key"]
    s3_tls_ca = microceph_credentials["tls-ca"]
    creds_secret_uri = juju.add_secret(
        "s3-creds", {"access-key": s3_access_key, "secret-key": s3_secret_key}
    )
    juju.grant_secret(creds_secret_uri, charm_versions.s3.application_name)
    juju.config(
        charm_versions.s3.application_name,
        {
            "endpoint": s3_endpoint,
            "bucket": BUCKET_NAME,
            "region": "",
            "s3-uri-style": "path",
            "path": f"{PATH_NAME}/",
            "tls-ca-chain": s3_tls_ca,
            "credentials": creds_secret_uri,
        },
    )
    juju.wait(lambda status: jubilant.all_active(status, charm_versions.s3.application_name))
    configure_s3_bucket(s3_endpoint, s3_access_key, s3_secret_key)


def test_external_service_account_not_monitored(
    juju: jubilant.Juju, service_account: tuple[str, str]
) -> None:
    """Check that service accounts are not monitored by default.

    This test makes sure that we only inject spark properties into a secret *after* we configure the
    integration hub to monitor a specific service account.
    """
    name, namespace = service_account
    juju.wait(jubilant.all_active, delay=5)
    assert not does_secret_exist(namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{name}")

    juju.config(APP_NAME, {"monitored-service-accounts": f"{namespace}:{name}"})
    juju.wait(jubilant.all_active, delay=5)
    assert does_secret_exist(namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{name}")


def test_relation_with_s3(
    juju: jubilant.Juju, service_account: tuple[str, str], charm_versions: IntegrationTestsCharms
) -> None:
    service_account_name = service_account[0]
    namespace = service_account[1]
    logger.info(f"Service account: {service_account_name}, namespace: {namespace}")
    juju.config(APP_NAME, {"monitored-service-accounts": f"{namespace}:{service_account_name}"})
    juju.wait(jubilant.all_active, delay=5)

    # Verify that secret data is empty before S3 relation is added.
    secret_data = get_secret_data(
        namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )
    logger.info(f"namespace: {namespace} -> secret_data: {secret_data}")
    assert len(secret_data) == 0

    # Relate S3 integrator with Spark Integration Hub
    juju.integrate(
        APP_NAME,
        charm_versions.s3.application_name,
    )
    juju.wait(jubilant.all_active, delay=5)

    secret_data = get_secret_data(
        namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )
    logger.info(f"namespace: {namespace} -> secret_data: {secret_data}")
    assert len(secret_data) > 0
    assert "spark.hadoop.fs.s3a.access.key" in secret_data

    truststore_secret_name = get_truststore_secret_name(model_name=juju.model, app_name=APP_NAME)
    logger.info(f"Truststore secret name: {truststore_secret_name}")
    secret_data_truststore = get_secret_data(
        namespace=namespace, secret_name=truststore_secret_name
    )
    logger.info(f"namespace: {namespace} -> secret_data: {secret_data_truststore}")
    assert len(secret_data_truststore) > 0

    logger.info("Setup spark-client snap.")
    setup_spark_output = subprocess.check_output(
        f"./tests/integration/setup/setup_spark.sh {service_account_name} {namespace}",
        shell=True,
        stderr=None,
    ).decode("utf-8")
    logger.info(f"Setup spark output:\n{setup_spark_output}")

    logger.info("Executing Spark job...")
    proc = subprocess.Popen(
        ["./tests/integration/setup/run_spark_job.sh", service_account_name, namespace],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    stdout, stderr = proc.communicate()

    logger.info(f"Spark job stdout:\n{stdout}")
    logger.info(f"Spark job stderr:\n{stderr}")
    logger.info("Spark job has ended!")

    assert "termination reason: Completed" in stderr

    logger.info("Waiting for 5 seconds...")
    time.sleep(5)


def test_new_service_account_with_s3(
    juju: jubilant.Juju, service_account: tuple[str, str], charm_versions: IntegrationTestsCharms
) -> None:
    logger.info(
        "Testing that new service accounts also get the TLS configuration once S3 relation is added."
    )
    service_account_name = service_account[0]
    namespace = service_account[1]
    logger.info("HERE")
    logger.info(f"Service account: {service_account_name}, namespace: {namespace}")

    juju.config(APP_NAME, {"monitored-service-accounts": f"{namespace}:{service_account_name}"})
    juju.wait(jubilant.all_active, delay=10, timeout=120)

    # check secret
    secret_data = get_secret_data(
        namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )
    logger.info(f"namespace: {namespace} -> secret_data: {secret_data}")
    assert len(secret_data) > 0
    assert "spark.hadoop.fs.s3a.access.key" in secret_data

    truststore_secret_name = get_truststore_secret_name(model_name=juju.model, app_name=APP_NAME)
    secret_data_truststore = get_secret_data(
        namespace=namespace, secret_name=truststore_secret_name
    )
    logger.info(f"namespace: {namespace} -> secret_data: {secret_data_truststore}")
    assert len(secret_data_truststore) > 0

    logger.info(
        "Remove integration with S3 to check that secrets are properly deleted and recreated..."
    )
    # Removing S3 <> Integration Hub relation
    juju.remove_relation(APP_NAME, charm_versions.s3.application_name)
    juju.wait(jubilant.all_active, delay=10, timeout=120)

    secret_data = get_secret_data(
        namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )
    secret_data_truststore = get_secret_data(
        namespace=namespace, secret_name=truststore_secret_name
    )
    logger.info(f"namespace: {namespace} -> secret_data: {secret_data_truststore}")

    assert len(secret_data) == 0
    assert len(secret_data_truststore) == 0

    # Re-integrate S3 integrator with Spark Integration Hub
    juju.integrate(
        APP_NAME,
        charm_versions.s3.application_name,
    )
    juju.wait(jubilant.all_active, delay=5, timeout=120)
    secret_data = get_secret_data(
        namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )
    logger.info(f"namespace: {namespace} -> secret_data: {secret_data}")
    assert len(secret_data) > 0
    assert "spark.hadoop.fs.s3a.access.key" in secret_data

    secret_data_truststore = get_secret_data(
        namespace=namespace, secret_name=truststore_secret_name
    )
    logger.info(f"namespace: {namespace} -> secret_data: {secret_data_truststore}")
    assert len(secret_data_truststore) > 0


def test_correct_tls_in_manifest(
    juju: jubilant.Juju,
    test_charm: Path,
    namespace: str,
) -> None:
    """Test that the TLS secret manifest is correctly generated and contains the expected content."""
    logger.info("Deploying test application charm")
    juju.deploy(charm=test_charm, app=DUMMY_APP_NAME, num_units=1, base="ubuntu@24.04")

    juju.wait(
        lambda status: jubilant.all_active(status) and jubilant.all_agents_idle(status), delay=5
    )

    configuration_parameters = {"namespace": namespace}

    logger.info(f"Setting config for test application charm: {configuration_parameters}...")
    juju.config(DUMMY_APP_NAME, configuration_parameters)
    juju.wait(
        lambda status: jubilant.all_active(status) and jubilant.all_agents_idle(status), delay=5
    )

    logger.info("Integrating integration hub with test application for service account sa1")
    juju.integrate(APP_NAME, f"{DUMMY_APP_NAME}:{REL_NAME_A}")
    juju.wait(
        lambda status: jubilant.all_active(status) and jubilant.all_agents_idle(status),
        delay=5,
        timeout=120,
    )

    logger.info("Enable autoscaling...")
    juju.config(APP_NAME, {"enable-dynamic-allocation": "true"})
    juju.wait(
        lambda status: jubilant.all_active(status) and jubilant.all_agents_idle(status), delay=5
    )

    # The added spark properties be reflected on the requirer charm
    task = juju.run(f"{DUMMY_APP_NAME}/0", "get-properties-sa1")
    assert task.return_code == 0
    assert "spark-properties" in task.results
    properties = task.results["spark-properties"]
    assert "spark.dynamicAllocation.enabled" in json.loads(properties)

    task = juju.run(f"{DUMMY_APP_NAME}/0", "get-resource-manifest-sa1")
    assert task.return_code == 0
    assert "resource-manifest" in task.results
    manifest = task.results["resource-manifest"]
    assert manifest is not None
    assert manifest.strip() != ""
    logger.info(f"Generated manifest:\n{manifest}")

    assert "truststore.jks" in manifest


def test_remove_application(
    juju: jubilant.Juju,
    service_account: tuple[str, str],
) -> None:
    service_account_name = service_account[0]
    namespace = service_account[1]
    logger.info(f"Service account: {service_account_name}, namespace: {namespace}")

    juju.config(APP_NAME, {"monitored-service-accounts": f"{namespace}:{service_account_name}"})
    juju.wait(jubilant.all_active, delay=5)

    secret_data = get_secret_data(
        namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )
    logger.info(f"namespace: {namespace} -> secret_data: {secret_data}")
    assert len(secret_data) > 0
    assert "spark.hadoop.fs.s3a.access.key" in secret_data

    # Removing Spark Integration Hub application
    juju.remove_application(APP_NAME)
    juju.wait(jubilant.all_active, delay=5)

    secret_data = get_secret_data(
        namespace=namespace, secret_name=f"{SECRET_NAME_PREFIX}{service_account_name}"
    )
    logger.info(f"secret data: {secret_data}")
    assert len(secret_data) == 0
    truststore_secret_name = get_truststore_secret_name(model_name=juju.model, app_name=APP_NAME)
    secret_data_truststore = get_secret_data(
        namespace=namespace, secret_name=truststore_secret_name
    )
    logger.info(f"namespace: {namespace} -> secret_data: {secret_data_truststore}")
    assert len(secret_data_truststore) == 0
