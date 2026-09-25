#!/usr/bin/env python3
# Copyright 2026 Canonical Limited
# See LICENSE file for licensing details.

import base64
import logging
from pathlib import Path

import jubilant
import yaml

from ..types import IntegrationTestsCharms, S3Info

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]

logger = logging.getLogger(__name__)
logging.getLogger("jubilant.wait").setLevel(logging.WARNING)


def get_certificate_from_file(filename: str) -> str:
    """Returns the certificate as a string."""
    with open(filename, "r") as file:
        certificate = file.read()
    return certificate


def prepare_s3_storage_setup(
    juju: jubilant.Juju,
    charm_versions: IntegrationTestsCharms,
    s3_credentials: S3Info,
    integrate: bool = True,
    s3_tls: bool = False,
):
    """Prepare the S3 storage setup for the history server.

    Args:
        juju: The Juju client instance.
        charm_versions: The versions of the charms to deploy.
        s3_credentials: The S3 bucket and credentials information.
        integrate: Whether to integrate the S3 setup with the integration hub charm.
        s3_tls: Whether to enable TLS for the S3 connection.
    """
    bucket = s3_credentials["bucket"]
    access_key = s3_credentials["access_key"]
    secret_key = s3_credentials["secret_key"]
    endpoint = s3_credentials["endpoint"]
    path = s3_credentials["path"]
    tls_ca_chain_path = s3_credentials["ca_bundle_path"]

    logger.info("Deploying S3 Integrator charm")
    juju.deploy(**charm_versions.s3.deploy_dict())

    logger.info("Setting up s3 credentials in s3-integrator charm")
    secret_params = {
        "access-key": access_key,
        "secret-key": secret_key,
    }
    secret_uri = juju.add_secret("s3-credentials", secret_params)
    juju.grant_secret(secret_uri, charm_versions.s3.application_name)

    configuration_parameters = {
        "bucket": bucket,
        "path": path,
        "endpoint": endpoint,
        "credentials": secret_uri,
    }
    if s3_tls:
        ca = get_certificate_from_file(tls_ca_chain_path)
        ca_b64 = base64.b64encode(ca.encode("utf-8")).decode("utf-8")
        configuration_parameters["tls-ca-chain"] = ca_b64

    juju.config(charm_versions.s3.application_name, configuration_parameters)
    juju.wait(
        lambda status: (
            jubilant.all_active(status, charm_versions.s3.application_name)
            and jubilant.all_agents_idle(status)
        )
    )
    if integrate:
        logger.info("Integrating integration hub charm with s3-integrator charm")
        juju.integrate(APP_NAME, charm_versions.s3.application_name)

    juju.wait(jubilant.all_active)
    logger.info("S3 storage setup completed")
