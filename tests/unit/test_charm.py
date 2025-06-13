# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

import json
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml
from ops import ActiveStatus, BlockedStatus, MaintenanceStatus
from ops.testing import Container, Context, Relation, State

from charm import SparkIntegrationHub
from constants import CONTAINER

CONFIG = yaml.safe_load(Path("./config.yaml").read_text())
METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())


@pytest.fixture()
def charm_configuration():
    """Enable direct mutation on configuration dict."""
    return json.loads(json.dumps(CONFIG))


def parse_spark_properties(out: State, tmp_path: Path) -> dict[str, str]:
    spark_properties_path = (
        out.get_container(CONTAINER)
        .layers["base"]
        .services["integration-hub"]
        .environment["SPARK_PROPERTIES_FILE"]
    )

    file_path = tmp_path / Path(spark_properties_path).relative_to("/etc")

    assert file_path.exists()

    with file_path.open("r") as fid:
        return dict(
            row.rsplit("=", maxsplit=1) for line in fid.readlines() if (row := line.strip())
        )


def test_start_integration_hub(integration_hub_ctx: Context[SparkIntegrationHub]) -> None:
    state = State(
        config={},
        containers=[Container(name=CONTAINER, can_connect=False)],
    )
    out = integration_hub_ctx.run(integration_hub_ctx.on.install(), state)
    assert out.unit_status == MaintenanceStatus("Waiting for Pebble")


@patch("workload.IntegrationHub.exec")
def test_pebble_ready(
    exec_calls,
    integration_hub_ctx: Context[SparkIntegrationHub],
    integration_hub_container: Container,
) -> None:
    state = State(
        containers=[integration_hub_container],
    )
    with (
        patch("managers.k8s.KubernetesManager.__init__", return_value=None),
        patch("managers.k8s.KubernetesManager.trusted", return_value=True),
    ):
        out = integration_hub_ctx.run(
            integration_hub_ctx.on.pebble_ready(integration_hub_container), state
        )
        assert out.unit_status == ActiveStatus("")


@patch("managers.s3.S3Manager.verify", return_value=True)
@patch("workload.IntegrationHub.exec")
def test_s3_relation_connection_ok(
    exec_calls,
    verify_call,
    tmp_path: Path,
    integration_hub_ctx: Context[SparkIntegrationHub],
    integration_hub_container: Container,
    s3_relation: Relation,
) -> None:
    state = State(
        relations=[s3_relation],
        containers=[integration_hub_container],
    )
    with (
        patch("managers.k8s.KubernetesManager.__init__", return_value=None),
        patch("managers.k8s.KubernetesManager.trusted", return_value=True),
    ):
        out = integration_hub_ctx.run(integration_hub_ctx.on.relation_changed(s3_relation), state)
        assert out.unit_status == ActiveStatus("")

        # Check containers modifications
        assert len(out.get_container(CONTAINER).layers) == 2

        envs = (
            out.get_container(CONTAINER)
            .layers["integration-hub"]
            .services["integration-hub"]
            .environment
        )

        assert "SPARK_PROPERTIES_FILE" in envs

        spark_properties = parse_spark_properties(out, tmp_path)

        # Assert one of the keys
        assert "spark.hadoop.fs.s3a.endpoint" in spark_properties
        assert (
            spark_properties["spark.hadoop.fs.s3a.endpoint"]
            == s3_relation.remote_app_data["endpoint"]
        )


@patch("managers.s3.S3Manager.verify", return_value=True)
@patch("workload.IntegrationHub.exec")
def test_s3_relation_connection_ok_tls(
    exec_calls,
    verify_call,
    tmp_path: Path,
    integration_hub_ctx: Context[SparkIntegrationHub],
    integration_hub_container: Container,
    s3_relation_tls: Relation,
    s3_relation: Relation,
) -> None:
    state = State(
        relations=[s3_relation_tls],
        containers=[integration_hub_container],
    )

    with (
        patch("managers.k8s.KubernetesManager.__init__", return_value=None),
        patch("managers.k8s.KubernetesManager.trusted", return_value=True),
    ):
        inter = integration_hub_ctx.run(
            integration_hub_ctx.on.relation_changed(s3_relation_tls), state
        )
        assert inter.unit_status == ActiveStatus("")

        # Check containers modifications
        assert len(inter.get_container(CONTAINER).layers) == 2
        spark_properties = parse_spark_properties(inter, tmp_path)

        # Assert one of the keys
        assert "spark.hadoop.fs.s3a.endpoint" in spark_properties
        assert (
            spark_properties["spark.hadoop.fs.s3a.endpoint"]
            == s3_relation_tls.remote_app_data["endpoint"]
        )

        out = integration_hub_ctx.run(
            integration_hub_ctx.on.relation_changed(s3_relation),
            replace(inter, relations=[s3_relation]),
        )

        assert len(out.get_container(CONTAINER).layers) == 2


@patch("managers.s3.S3Manager.verify", return_value=False)
@patch("workload.IntegrationHub.exec")
def test_s3_relation_connection_ko(
    exec_calls,
    verify_call,
    integration_hub_ctx: Context[SparkIntegrationHub],
    integration_hub_container: Container,
    s3_relation: Relation,
) -> None:
    state = State(
        relations=[s3_relation],
        containers=[integration_hub_container],
    )
    with (
        patch("managers.k8s.KubernetesManager.__init__", return_value=None),
        patch("managers.k8s.KubernetesManager.trusted", return_value=True),
    ):
        out = integration_hub_ctx.run(integration_hub_ctx.on.relation_changed(s3_relation), state)
        assert out.unit_status == BlockedStatus("Invalid S3 credentials")


@patch("managers.s3.S3Manager.verify", return_value=True)
@patch("workload.IntegrationHub.exec")
def test_s3_relation_broken(
    exec_calls,
    verify_call,
    tmp_path: Path,
    integration_hub_ctx: Context[SparkIntegrationHub],
    integration_hub_container: Container,
    s3_relation: Relation,
) -> None:
    initial_state = State(
        relations=[s3_relation],
        containers=[integration_hub_container],
    )

    with (
        patch("managers.k8s.KubernetesManager.__init__", return_value=None),
        patch("managers.k8s.KubernetesManager.trusted", return_value=True),
    ):
        state_after_relation_changed = integration_hub_ctx.run(
            integration_hub_ctx.on.relation_changed(s3_relation), initial_state
        )
        state_after_relation_broken = integration_hub_ctx.run(
            integration_hub_ctx.on.relation_broken(s3_relation), state_after_relation_changed
        )

        assert state_after_relation_broken.unit_status == ActiveStatus("")

        spark_properties = parse_spark_properties(state_after_relation_broken, tmp_path)

        # Assert one of the keys
        assert "spark.hadoop.fs.s3a.endpoint" not in spark_properties


@patch("managers.s3.S3Manager.verify", return_value=True)
@patch("workload.IntegrationHub.exec")
@patch("ops.JujuVersion.has_secrets", return_value=True)
def test_azure_storage_relation(
    mock_has_secrets,
    exec_calls,
    verify_call,
    tmp_path: Path,
    integration_hub_ctx: Context[SparkIntegrationHub],
    integration_hub_container: Container,
    azure_storage_relation: Relation,
) -> None:
    state = State(
        relations=[azure_storage_relation],
        containers=[integration_hub_container],
    )
    with (
        patch("managers.k8s.KubernetesManager.__init__", return_value=None),
        patch("managers.k8s.KubernetesManager.trusted", return_value=True),
    ):
        out = integration_hub_ctx.run(
            integration_hub_ctx.on.relation_changed(azure_storage_relation), state
        )
        assert out.unit_status == ActiveStatus("")

        # Check containers modifications
        assert len(out.get_container(CONTAINER).layers) == 2

        envs = (
            out.get_container(CONTAINER)
            .layers["integration-hub"]
            .services["integration-hub"]
            .environment
        )

        assert "SPARK_PROPERTIES_FILE" in envs

        spark_properties = parse_spark_properties(out, tmp_path)

        # Assert one of the keys
        storage_account = azure_storage_relation.remote_app_data["storage-account"]
        assert (
            f"spark.hadoop.fs.azure.account.key.{storage_account}.dfs.core.windows.net"
            in spark_properties
        )
        assert (
            spark_properties[
                f"spark.hadoop.fs.azure.account.key.{storage_account}.dfs.core.windows.net"
            ]
            == azure_storage_relation.remote_app_data["secret-key"]
        )


@patch("managers.s3.S3Manager.verify", return_value=True)
@patch("workload.IntegrationHub.exec")
@patch("ops.JujuVersion.has_secrets", return_value=True)
def test_azure_storage_relation_broken(
    mock_has_secrets,
    exec_calls,
    verify_call,
    tmp_path: Path,
    integration_hub_ctx: Context[SparkIntegrationHub],
    integration_hub_container: Container,
    azure_storage_relation: Relation,
) -> None:
    state = State(
        relations=[azure_storage_relation],
        containers=[integration_hub_container],
    )
    with (
        patch("managers.k8s.KubernetesManager.__init__", return_value=None),
        patch("managers.k8s.KubernetesManager.trusted", return_value=True),
    ):
        state_after_relation_broken = integration_hub_ctx.run(
            integration_hub_ctx.on.relation_broken(azure_storage_relation),
            state,
        )

    assert state_after_relation_broken.unit_status == ActiveStatus("")

    spark_properties = parse_spark_properties(state_after_relation_broken, tmp_path)

    storage_account = azure_storage_relation.remote_app_data["storage-account"]
    assert (
        f"spark.hadoop.fs.azure.account.key.{storage_account}.dfs.core.windows.net"
        not in spark_properties
    )


@patch("managers.s3.S3Manager.verify", return_value=True)
@patch("workload.IntegrationHub.exec")
@patch("ops.JujuVersion.has_secrets", return_value=True)
def test_both_azure_storage_and_s3_relation_together(
    mock_has_secrets,
    exec_calls,
    verify_call,
    integration_hub_ctx: Context[SparkIntegrationHub],
    integration_hub_container: Container,
    s3_relation: Relation,
    azure_storage_relation: Relation,
) -> None:
    state = State(
        relations=[s3_relation, azure_storage_relation],
        containers=[integration_hub_container],
    )
    with (
        patch("managers.k8s.KubernetesManager.__init__", return_value=None),
        patch("managers.k8s.KubernetesManager.trusted", return_value=True),
    ):
        out = integration_hub_ctx.run(
            integration_hub_ctx.on.relation_changed(azure_storage_relation), state
        )
        assert out.unit_status == BlockedStatus(
            "Integration Hub can be related to only one storage backend at a time."
        )


@patch("workload.IntegrationHub.exec")
def test_logging_relation_changed(
    exec_calls,
    tmp_path: Path,
    integration_hub_ctx: Context[SparkIntegrationHub],
    integration_hub_container: Container,
    logging_relation: Relation,
) -> None:
    """Test logging relation changed."""
    exp_url = "http://grafana-agent-k8s-0.grafana-agent-k8s-endpoints.spark.svc.cluster.local:3500/loki/api/v1/push"
    state = State(relations=[logging_relation], containers=[integration_hub_container])

    with (
        patch("managers.k8s.KubernetesManager.__init__", return_value=None),
        patch("managers.k8s.KubernetesManager.trusted", return_value=True),
    ):
        out = integration_hub_ctx.run(
            integration_hub_ctx.on.relation_changed(logging_relation), state
        )

    assert out.unit_status == ActiveStatus("")

    # Check containers modifications
    assert len(out.get_container(CONTAINER).layers) == 3

    envs = (
        out.get_container(CONTAINER)
        .layers["integration-hub"]
        .services["integration-hub"]
        .environment
    )

    assert "SPARK_PROPERTIES_FILE" in envs

    spark_properties = parse_spark_properties(out, tmp_path)
    assert spark_properties.get("spark.executorEnv.LOKI_URL") == exp_url
    assert spark_properties.get("spark.kubernetes.driverEnv.LOKI_URL") == exp_url


@patch("workload.IntegrationHub.exec")
def test_logging_relation_broken(
    exec_calls,
    tmp_path: Path,
    integration_hub_ctx: Context[SparkIntegrationHub],
    integration_hub_container: Container,
    logging_relation: Relation,
) -> None:
    """Test logging relation broken."""
    state = State(relations=[logging_relation], containers=[integration_hub_container])

    with (
        patch("managers.k8s.KubernetesManager.__init__", return_value=None),
        patch("managers.k8s.KubernetesManager.trusted", return_value=True),
    ):
        after_join_state = integration_hub_ctx.run(
            integration_hub_ctx.on.relation_changed(logging_relation), state
        )  # relation changed
        out = integration_hub_ctx.run(
            integration_hub_ctx.on.relation_broken(logging_relation), after_join_state
        )  # relation broken

    assert out.unit_status == ActiveStatus("")

    # Check containers modifications
    assert len(out.get_container(CONTAINER).layers) == 3

    envs = (
        out.get_container(CONTAINER)
        .layers["integration-hub"]
        .services["integration-hub"]
        .environment
    )

    assert "SPARK_PROPERTIES_FILE" in envs

    spark_properties = parse_spark_properties(out, tmp_path)
    assert "spark.executorEnv.LOKI_URL" not in spark_properties
    assert "spark.kubernetes.driverEnv.LOKI_URL" not in spark_properties


@patch("workload.IntegrationHub.exec")
def test_config_changed_properties_updated(
    exec_calls, tmp_path: Path, integration_hub_container: Container, charm_configuration: dict
) -> None:
    """Test configuration flags."""
    # Given
    charm_configuration["options"]["driver-pod-template"]["default"] = "s3://my-bucket/driver.yaml"
    charm_configuration["options"]["executor-pod-template"]["default"] = (
        "s3://my-bucket/executor.yaml"
    )
    charm_configuration["options"]["enable-dynamic-allocation"]["default"] = "true"
    ctx = Context(SparkIntegrationHub, meta=METADATA, config=charm_configuration, unit_id=0)
    state = State(relations=[], containers=[integration_hub_container])

    # When
    with (
        patch("managers.k8s.KubernetesManager.__init__", return_value=None),
        patch("managers.k8s.KubernetesManager.trusted", return_value=True),
    ):
        out = ctx.run(ctx.on.config_changed(), state)  # relation changed

    # Then
    spark_properties = parse_spark_properties(out, tmp_path)
    assert (
        spark_properties.get("spark.kubernetes.driver.podTemplateFile", "")
        == "s3://my-bucket/driver.yaml"
    )
    assert (
        spark_properties.get("spark.kubernetes.executor.podTemplateFile", "")
        == "s3://my-bucket/executor.yaml"
    )
    assert "spark.dynamicAllocation.enabled" in spark_properties
