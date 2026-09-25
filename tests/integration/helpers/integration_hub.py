#!/usr/bin/env python3
# Copyright 2026 Canonical Limited
# See LICENSE file for licensing details.

import base64
import hashlib
import logging
from pathlib import Path

import jubilant
import yaml
from lightkube import ApiError, Client
from lightkube.resources.core_v1 import Secret
from spark8t.literals import HUB_LABEL
from spark8t.utils import K8sSecretKeySerializer

from ..types import AzureInfo, IntegrationTestsCharms, S3Info
from .azure_storage import prepare_azure_storage_setup
from .s3 import prepare_s3_storage_setup

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]
TEST_CHARM_APP_NAME = "app"
TEST_CHARM_RELATION_A_NAME = "spark-account-a"
SECRET_NAME_PREFIX = "integrator-hub-conf-"


# Label the integration hub stamps on the Kubernetes resources it manages.
MANAGED_BY_LABEL = "app.kubernetes.io/managed-by"
MANAGED_BY_INTEGRATION_HUB = "integration-hub"


logger = logging.getLogger(__name__)
logging.getLogger("jubilant.wait").setLevel(logging.WARNING)


def get_integration_hub_secret(
    namespace: str,
    service_account: str,
) -> Secret | None:
    """Return the integration hub secret for the given service account, if it exists."""
    client = Client()
    try:
        secret = client.get(Secret, name=f"{HUB_LABEL}-{service_account}", namespace=namespace)
        labels = (secret.metadata.labels or {}) if secret.metadata else {}
        if labels.get(MANAGED_BY_LABEL) != MANAGED_BY_INTEGRATION_HUB:
            return None
        return secret
    except ApiError:
        return None


def get_truststore_secret(
    model_name: str,
    app_name: str,
    namespace: str,
) -> Secret | None:
    """Return the truststore secret for the given service account, if it exists."""
    client = Client()
    try:
        suffix = hashlib.sha256(f"{model_name}|{app_name}".encode()).hexdigest()[:8]
        secret_name = f"{SECRET_NAME_PREFIX}truststore-{suffix}"
        secret = client.get(Secret, name=secret_name, namespace=namespace)
        labels = (secret.metadata.labels or {}) if secret.metadata else {}
        if labels.get(MANAGED_BY_LABEL) != MANAGED_BY_INTEGRATION_HUB:
            return None
        return secret
    except ApiError:
        return None


def get_integration_hub_secret_data(
    namespace: str,
    service_account: str,
) -> dict[str, str]:
    hub_secret = get_integration_hub_secret(namespace, service_account)
    if not hub_secret:
        return {}
    if not hub_secret.data:
        return {}
    spark_properties = {
        K8sSecretKeySerializer().deserialize(k): base64.b64decode(v).decode("utf-8")
        for k, v in hub_secret.data.items()
    }
    return spark_properties


def get_truststore_secret_data(
    model_name: str,
    app_name: str,
    namespace: str,
) -> dict[str, bytes]:
    truststore_secret = get_truststore_secret(
        model_name=model_name, app_name=app_name, namespace=namespace
    )
    if not truststore_secret:
        return {}
    if not truststore_secret.data:
        return {}
    return {
        K8sSecretKeySerializer().deserialize(k): base64.b64decode(v)
        for k, v in truststore_secret.data.items()
    }


def integration_hub_secret_exists(
    workload_namespace: str,
    workload_service_account: str,
    with_properties: dict[str, str] | None = None,
) -> bool:
    """Whether the integration hub config secret for a service account is in place.

    Matches on behaviour: a Secret in `workload_namespace` managed by the
    integration hub (via its managed-by label) whose name is derived from the
    service account, and whose data pins it to the given namespace and service
    account.
    """
    hub_secret = get_integration_hub_secret(
        namespace=workload_namespace, service_account=workload_service_account
    )
    if not hub_secret:
        return False
    if not with_properties:
        return True
    spark_properties = get_integration_hub_secret_data(
        namespace=workload_namespace, service_account=workload_service_account
    )
    if all(spark_properties.get(k) == v for k, v in with_properties.items()):
        return True
    return False


def deploy_integration_hub_setup(
    juju: jubilant.Juju,
    hub_charm: str | Path,
    charm_versions: IntegrationTestsCharms,
    s3_credentials: S3Info | None = None,
    azure_storage_credentials: AzureInfo | None = None,
    s3_tls: bool = False,
    monitored_service_accounts: str = "",
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
        "config": {"monitored-service-accounts": monitored_service_accounts},
        "trust": trust,
    }

    logger.info("Deploying integration hub charm...")
    juju.deploy(hub_charm, **deploy_args)

    logger.info("Waiting for integration hub app to settle...")
    juju.wait(jubilant.all_active)

    if s3_credentials is not None:
        logger.info("Using S3 object storage with Integration Hub")
        prepare_s3_storage_setup(
            juju=juju, charm_versions=charm_versions, s3_credentials=s3_credentials, s3_tls=s3_tls
        )
    elif azure_storage_credentials is not None:
        logger.info("Using Azure object storage with Integration Hub")
        prepare_azure_storage_setup(
            juju=juju,
            charm_versions=charm_versions,
            azure_storage_credentials=azure_storage_credentials,
        )

    juju.wait(jubilant.all_active)
    logger.info("Successfully deployed Integration Hub setup.")


def deploy_test_charm_setup(
    juju: jubilant.Juju,
    test_charm: str | Path,
    spark_workload_namespace: str,
    integrate: bool = True,
) -> None:
    logger.info("Deploying test charm...")
    juju.deploy(test_charm, app=TEST_CHARM_APP_NAME)
    juju.wait(lambda status: jubilant.all_agents_idle(status), delay=5)

    logger.info("Configuring test charm...")
    juju.config(
        TEST_CHARM_APP_NAME,
        {
            "namespace": spark_workload_namespace,
        },
    )
    juju.wait(
        lambda status: (
            jubilant.all_active(status, TEST_CHARM_APP_NAME) and jubilant.all_agents_idle(status)
        ),
        delay=5,
    )
    if integrate:
        logger.info("Integrating integration hub with test application for service account sa1")
        juju.integrate(APP_NAME, f"{TEST_CHARM_APP_NAME}:{TEST_CHARM_RELATION_A_NAME}")
        juju.wait(
            lambda status: (
                jubilant.all_active(status, APP_NAME, TEST_CHARM_APP_NAME)
                and jubilant.all_agents_idle(status)
            ),
            delay=15,
        )
