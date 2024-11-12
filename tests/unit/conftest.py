# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

import pytest
from ops import pebble
from scenario import Container, Context, Model, Mount, Relation
from scenario.state import next_relation_id

from charm import SparkIntegrationHub
from constants import CONTAINER, LOGGING_RELATION_NAME
from core.context import AZURE_RELATION_NAME, S3_RELATION_NAME


@pytest.fixture
def integration_hub_charm():
    """Provide fixture for the SparkIntegrationHub charm."""
    yield SparkIntegrationHub


@pytest.fixture
def integration_hub_ctx(integration_hub_charm):
    """Provide fixture for scenario context based on the SparkIntegrationHub charm."""
    return Context(
        charm_type=integration_hub_charm,
        juju_version="3.4.2",  # Note(rgildein): Pebble LogForwarding require Juju 3.4 or higher
    )


@pytest.fixture
def model():
    """Provide fixture for the testing Juju model."""
    return Model(name="test-model")


@pytest.fixture
def integration_hub_container(tmp_path):
    """Provide fixture for the Integration Hub workload container."""
    layer = pebble.Layer(
        {
            "summary": "Charmed Spark Integration Hub",
            "description": "Pebble base layer in Charmed Spark Integration Hub",
            "services": {
                "integration-hub": {
                    "override": "replace",
                    "summary": "This is the Spark Integration Hub service",
                    "command": "/bin/bash /opt/hub/monitor_sa.sh",
                    "startup": "disabled",
                    "environment": {
                        "SPARK_PROPERTIES_FILE": "/etc/hub/conf/spark-properties.conf"
                    },
                },
            },
        }
    )

    etc = Mount("/etc/", tmp_path)

    return Container(
        name=CONTAINER,
        can_connect=True,
        layers={"base": layer},
        service_status={"integration-hub": pebble.ServiceStatus.ACTIVE},
        mounts={"etc": etc},
    )


@pytest.fixture
def s3_relation():
    """Provide fixture for the S3 relation."""
    relation_id = next_relation_id(update=True)

    return Relation(
        endpoint=S3_RELATION_NAME,
        interface="s3",
        remote_app_name="s3-integrator",
        relation_id=relation_id,
        local_app_data={"bucket": f"relation-{relation_id}"},
        remote_app_data={
            "access-key": "access-key",
            "bucket": "my-bucket",
            "data": f'{{"bucket": "relation-{relation_id}"}}',
            "endpoint": "https://s3.endpoint",
            "path": "spark-events",
            "secret-key": "secret-key",
        },
    )


@pytest.fixture
def azure_storage_relation():
    """Provide fixture for the Azure storage relation."""
    relation_id = next_relation_id(update=True)

    return Relation(
        endpoint=AZURE_RELATION_NAME,
        interface="azure",
        remote_app_name="azure-storage-integrator",
        relation_id=relation_id,
        local_app_data={"container": f"relation-{relation_id}"},
        remote_app_data={
            "container": "my-bucket",
            "data": f'{{"container": "relation-{relation_id}"}}',
            "path": "spark-events",
            "storage-account": "test-storage-account",
            "connection-protocol": "abfss",
            "secret-key": "some-secret",
        },
    )


@pytest.fixture
def s3_relation_tls():
    """Provide fixture for the S3 relation."""
    relation_id = next_relation_id(update=True)

    return Relation(
        endpoint=S3_RELATION_NAME,
        interface="s3",
        remote_app_name="s3-integrator",
        relation_id=relation_id,
        local_app_data={"bucket": f"relation-{relation_id}"},
        remote_app_data={
            "access-key": "access-key",
            "bucket": "my-bucket",
            "data": f'{{"bucket": "relation-{relation_id}"}}',
            "endpoint": "https://s3.endpoint",
            "path": "spark-events",
            "secret-key": "secret-key",
            "tls-ca-chain": '["certificate"]',
        },
    )


@pytest.fixture
def logging_relation():
    """Provide fixture for the logging relation."""
    relation_id = next_relation_id(update=True)

    return Relation(
        endpoint=LOGGING_RELATION_NAME,
        interface="logging",
        remote_app_name="grafana-agent-k8s",
        relation_id=relation_id,
        remote_app_data={"promtail_binary_zip_url": "{}"},
        remote_units_data={
            0: {
                "endpoint": '{"url": "http://grafana-agent-k8s-0.grafana-agent-k8s-endpoints.spark.svc.cluster.local:3500/loki/api/v1/push"}'
            }
        },
    )
