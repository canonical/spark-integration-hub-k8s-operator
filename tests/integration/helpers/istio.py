import logging
import subprocess
from pathlib import Path
from typing import cast

import jubilant
import yaml
from lightkube import Client
from lightkube.generic_resource import create_namespaced_resource

from ..types import IntegrationTestsCharms
from .integration_hub import MANAGED_BY_INTEGRATION_HUB, MANAGED_BY_LABEL

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]

AuthorizationPolicy = create_namespaced_resource(
    group="security.istio.io",
    version="v1",
    kind="AuthorizationPolicy",
    plural="authorizationpolicies",
)


def _workload_principal(workload_namespace: str, workload_service_account: str) -> str:
    """Build the SPIFFE principal for a workload service account."""
    return f"cluster.local/ns/{workload_namespace}/sa/{workload_service_account}"


def _policy_selector_labels(policy) -> dict[str, str]:
    return (policy.spec or {}).get("selector", {}).get("matchLabels", {})


def _is_managed_by_integration_hub(policy) -> bool:
    labels = (policy.metadata.labels or {}) if policy.metadata else {}
    return labels.get(MANAGED_BY_LABEL) == MANAGED_BY_INTEGRATION_HUB


def _policy_principals(policy) -> set[str]:
    principals: set[str] = set()
    for rule in (policy.spec or {}).get("rules", []):
        for source_rule in rule.get("from", []):
            principals.update(source_rule.get("source", {}).get("principals", []))
    return principals


def _workload_auth_policy_exists(
    workload_namespace: str, workload_service_account: str, role: str
) -> bool:
    """Check for an ALLOW policy selecting `spark-role=role` that allows the workload SA.

    Matches on behaviour (managed-by label, selector, action and allowed
    principal) rather than the generated policy name.
    """
    client = Client()
    principal = _workload_principal(workload_namespace, workload_service_account)
    for policy in client.list(AuthorizationPolicy, namespace=workload_namespace):
        spec = policy.spec or {}
        if not _is_managed_by_integration_hub(policy):
            continue
        if spec.get("action") != "ALLOW":
            continue
        if _policy_selector_labels(policy).get("spark-role") != role:
            continue
        if principal in _policy_principals(policy):
            return True
    return False


def driver_authorization_policy_exists(
    workload_namespace: str, workload_service_account: str
) -> bool:
    """Whether an authorization policy grants access to the driver of the workload SA."""
    return _workload_auth_policy_exists(workload_namespace, workload_service_account, "driver")


def executor_authorization_policy_exists(
    workload_namespace: str, workload_service_account: str
) -> bool:
    """Whether an authorization policy grants access to the executors of the workload SA."""
    return _workload_auth_policy_exists(workload_namespace, workload_service_account, "executor")


def client_application_authorization_policy_exists(
    workload_namespace: str,
    workload_service_account: str,
    client_app_namespace: str,
    client_app_service_account: str,
) -> bool:
    """Whether a policy in the client app namespace allows the workload SA to reach it.

    Matches on behaviour: an ALLOW policy in `client_app_namespace` selecting the
    client application pods and allowing the workload service account principal.
    """
    client = Client()
    principal = _workload_principal(workload_namespace, workload_service_account)
    for policy in client.list(AuthorizationPolicy, namespace=client_app_namespace):
        spec = policy.spec or {}
        if not _is_managed_by_integration_hub(policy):
            continue
        if spec.get("action") != "ALLOW":
            continue
        if (
            _policy_selector_labels(policy).get("app.kubernetes.io/name")
            != client_app_service_account
        ):
            continue
        if principal in _policy_principals(policy):
            return True
    return False


def deploy_istio_mesh_setup(
    juju: jubilant.Juju,
    charm_versions: IntegrationTestsCharms,
) -> None:
    """Deploy the Istio mesh setup."""
    logger.info("Deploying Istio K8s charm")
    juju.deploy(**charm_versions.istio.deploy_dict())
    juju.wait(
        lambda status: jubilant.all_active(status, charm_versions.istio.application_name), delay=5
    )

    logger.info("Deploying Istio beacon charm")
    juju.deploy(**charm_versions.istio_beacon.deploy_dict())
    juju.wait(
        lambda status: jubilant.all_active(status, charm_versions.istio_beacon.application_name),
        delay=5,
    )

    # TODO: Remove this hack, once https://github.com/canonical/service-mesh/issues/813 is fixed
    subprocess.run(
        [
            "kubectl",
            "create",
            "configmap",
            "-n",
            cast(str, juju.model),
            f"juju-service-mesh-{APP_NAME}-labels",
        ],
        check=True,
    )

    logger.info("Integrating Integration hub charm with istio beacon charm")
    juju.integrate(
        f"{APP_NAME}:service-mesh", f"{charm_versions.istio_beacon.application_name}:service-mesh"
    )
    juju.wait(lambda status: jubilant.all_active(status, APP_NAME), delay=5)
