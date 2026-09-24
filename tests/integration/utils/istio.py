import logging

import jubilant
import yaml
from ..types import IntegrationTestsCharms
from pathlib import Path

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]


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

    logger.info("Integrating Integration hub charm with istio beacon charm")
    juju.integrate(
        f"{APP_NAME}:service-mesh", f"{charm_versions.istio_beacon.application_name}:service-mesh"
    )
    juju.wait(lambda status: jubilant.all_active(status, APP_NAME), delay=5)
