import json
import logging
from datetime import datetime

import jubilant
from tenacity import retry, stop_after_attempt, wait_fixed

from ..types import IntegrationTestsCharms
from .integration_hub import APP_NAME
from .juju import get_related_unit_data

logger = logging.getLogger(__name__)


def deploy_observability_setup(juju: jubilant.Juju, charm_versions: IntegrationTestsCharms):
    logger.info("Deploying the grafana-agent-k8s charm")
    juju.deploy(**charm_versions.grafana_agent.deploy_dict())
    juju.wait(
        lambda status: jubilant.all_blocked(status, charm_versions.grafana_agent.application_name)
    )

    logger.info("Deploying the prometheus-pushgateway-k8s charm")
    juju.deploy(**charm_versions.pushgateway.deploy_dict())
    juju.wait(
        lambda status: jubilant.all_active(status, charm_versions.pushgateway.application_name)
    )

    logger.info("Relating spark integration hub charm with pushgateway charm")
    juju.integrate(
        APP_NAME,
        charm_versions.pushgateway.application_name,
    )
    juju.wait(
        lambda status: jubilant.all_active(
            status, APP_NAME, charm_versions.pushgateway.application_name
        ),
        delay=5,
    )

    logger.info(
        "Relating spark-integration-hub charm with grafana-agent-k8s charm over logging relation"
    )
    juju.integrate(
        f"{APP_NAME}:logging",
        charm_versions.grafana_agent.application_name,
    )
    juju.wait(
        lambda status: (
            jubilant.all_active(status, APP_NAME)
            and jubilant.all_blocked(status, charm_versions.grafana_agent.application_name)
        ),
        delay=5,
    )


@retry(
    wait=wait_fixed(3),
    stop=stop_after_attempt(10),
    reraise=True,
)
def assert_metrics_in_pushgateway(pushgateway_address: str) -> None:
    import json
    import urllib.request

    metrics = json.loads(
        urllib.request.urlopen(f"http://{pushgateway_address}:9091/api/v1/metrics").read()
    )
    logger.info(f"Metrics: {metrics} at time: {datetime.now()}")
    assert len(metrics["data"]) > 0


def get_loki_push_endpoint(
    juju: jubilant.Juju, logging_relation_name: str, remote_app_name: str
) -> str:
    loki_unit_data = get_related_unit_data(juju, APP_NAME, logging_relation_name, remote_app_name)
    endpoint_data = json.loads(loki_unit_data["endpoint"])
    return endpoint_data["url"]
