#!/usr/bin/env python3
# Copyright 2026 Canonical Limited
# See LICENSE file for licensing details.

import logging
from pathlib import Path

import jubilant
import yaml

from ..types import AzureInfo, IntegrationTestsCharms, S3Info
from .azure_storage import prepare_azure_storage_setup
from .s3 import prepare_s3_storage_setup

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]

logger = logging.getLogger(__name__)
logging.getLogger("jubilant.wait").setLevel(logging.WARNING)


def deploy_integration_hub_setup(
    juju: jubilant.Juju,
    hub_charm: str | Path,
    charm_versions: IntegrationTestsCharms,
    s3_credentials: S3Info | None = None,
    azure_storage_credentials: AzureInfo | None = None,
    s3_tls: bool = False,
    trust: bool = True,
) -> None:
    image_version = METADATA["resources"]["integration-hub-image"]["upstream-source"]
    resources = {"integration-hub-image": image_version}
    logger.info(f"Image version: {image_version}")
    deploy_args = {
        "app": APP_NAME,
        "num_units": 1,
        "base": "ubuntu@22.04",
        "resources": resources,
        "trust": trust,
    }

    logger.info("Deploying integration hub charm...")
    juju.deploy(hub_charm, **deploy_args)

    logger.info("Waiting for integration hub app to settle...")
    juju.wait(jubilant.all_active)

    if s3_credentials is not None:
        logger.info("Using S3 object storage with Integration Hub")
        prepare_s3_storage_setup(juju, charm_versions, s3_credentials, s3_tls=s3_tls)
    elif azure_storage_credentials is not None:
        logger.info("Using Azure object storage with Integration Hub")
        prepare_azure_storage_setup(juju, charm_versions, azure_storage_credentials)

    juju.wait(jubilant.all_active)
    logger.info("Successfully deployed Integration Hub setup.")
