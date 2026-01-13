# Copyright 2025 Canonical Limited
# See LICENSE file for licensing details.

import dataclasses
import json
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml
from ops import ActiveStatus
from ops.testing import Container, Context, Relation, State

from charm import SparkIntegrationHub

CONFIG = yaml.safe_load(Path("./config.yaml").read_text())
METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())


@pytest.fixture()
def charm_configuration():
    """Enable direct mutation on configuration dict."""
    return json.loads(json.dumps(CONFIG))


@patch("managers.s3.S3Manager.verify", return_value=True)
@patch("workload.IntegrationHub.exec", return_value="")
def test_props_serialization_in_resource_manifest(
    mock_exec_calls,
    mock_s3_verify,
    integration_hub_ctx: Context[SparkIntegrationHub],
    integration_hub_container: Container,
    pushgateway_relation: Relation,
    spark_service_account_provider_relation,
) -> None:
    """Test the prop keys are properly serialized, specially when the keys may contain asterisk (*) character."""
    state = State(
        relations=[spark_service_account_provider_relation],
        containers=[integration_hub_container],
        leader=True,
    )

    with (
        patch("managers.k8s.KubernetesManager.__init__", return_value=None),
        patch("managers.k8s.KubernetesManager.trusted", return_value=True),
    ):
        out = integration_hub_ctx.run(
            integration_hub_ctx.on.relation_changed(spark_service_account_provider_relation), state
        )

    assert out.unit_status == ActiveStatus("")

    relations = list(out.relations)
    relations.append(pushgateway_relation)
    state_in = dataclasses.replace(out, relations=relations)
    with (
        patch("managers.k8s.KubernetesManager.__init__", return_value=None),
        patch("managers.k8s.KubernetesManager.trusted", return_value=True),
    ):
        state_out = integration_hub_ctx.run(
            integration_hub_ctx.on.relation_changed(pushgateway_relation), state_in
        )

    assert state_out.unit_status == ActiveStatus("")

    app_data = state_out.get_relation(spark_service_account_provider_relation.id).local_app_data
    spark_properties = json.loads(app_data.get("spark-properties", "{}"))
    resource_manifest = yaml.safe_load(app_data.get("resource-manifest", ""))

    # The spark-properties keys should send as-is, without serialization
    assert "spark.metrics.conf.*.sink.prometheus.pushgateway-address" in spark_properties
    assert "spark.metrics.conf.*.sink.prometheus.class" in spark_properties

    # The resource-manifest keys should be serialized, so asterisk (*) becomes _2A
    assert "spark.metrics.conf.*.sink.prometheus.class" not in resource_manifest["stringData"]
    assert (
        "spark.metrics.conf.*.sink.prometheus.pushgateway-address"
        not in resource_manifest["stringData"]
    )
    assert (
        "spark.metrics.conf._2A.sink.prometheus.pushgateway-address"
        in resource_manifest["stringData"]
    )
    assert "spark.metrics.conf._2A.sink.prometheus.class" in resource_manifest["stringData"]
