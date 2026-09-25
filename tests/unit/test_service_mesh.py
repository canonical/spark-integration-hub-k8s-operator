#!/usr/bin/env python3
# Copyright 2026 Canonical Limited
# See LICENSE file for licensing details.

"""Unit tests for the service mesh feature."""

import json
from unittest.mock import patch

import pytest
from ops.testing import Container, Context, Relation, State

from charm import SparkIntegrationHub
from constants import (
    ISTIO_AMBIENT_LABEL_KEY,
    ISTIO_AMBIENT_LABEL_VALUE,
    SERVICE_MESH_RELATION_NAME,
)

LABEL_CONFIGMAP_NAME = "juju-service-mesh-spark-integration-hub-k8s-labels"
BEACON_LABELS = {ISTIO_AMBIENT_LABEL_KEY: ISTIO_AMBIENT_LABEL_VALUE}


@pytest.fixture
def service_mesh_relation() -> Relation:
    """Provide fixture for the service-mesh relation with istio-beacon-k8s."""
    return Relation(
        endpoint=SERVICE_MESH_RELATION_NAME,
        interface="service_mesh",
        remote_app_name="istio-beacon-k8s",
        remote_app_data={
            "labels": json.dumps(BEACON_LABELS),
            "mesh_type": json.dumps("istio"),
        },
    )


@patch("managers.k8s.KubernetesManager.trusted", return_value=True)
@patch("charmlibs.interfaces.service_mesh._service_mesh.reconcile_charm_labels")
@patch("workload.IntegrationHub.exec", return_value="")
def test_service_mesh_relation_adds_labels(
    mock_exec,
    reconcile_charm_labels,
    mock_trusted,
    integration_hub_ctx: Context[SparkIntegrationHub],
    integration_hub_container: Container,
    service_mesh_relation: Relation,
) -> None:
    """Joining the service-mesh relation applies the beacon's ambient labels to the charm pods."""
    state = State(
        leader=True,
        relations=[service_mesh_relation],
        containers=[integration_hub_container],
    )

    integration_hub_ctx.run(integration_hub_ctx.on.relation_changed(service_mesh_relation), state)

    reconcile_charm_labels.assert_called_once()
    assert reconcile_charm_labels.call_args.kwargs["labels"] == BEACON_LABELS
    assert reconcile_charm_labels.call_args.kwargs["label_configmap_name"] == LABEL_CONFIGMAP_NAME


@patch("managers.k8s.KubernetesManager.trusted", return_value=True)
@patch("lightkube.Client")
@patch("charmlibs.interfaces.service_mesh._service_mesh.reconcile_charm_labels")
@patch("workload.IntegrationHub.exec", return_value="")
def test_service_mesh_relation_broken_removes_labels(
    mock_exec,
    reconcile_charm_labels,
    mock_lightkube_client,
    mock_trusted,
    integration_hub_ctx: Context[SparkIntegrationHub],
    integration_hub_container: Container,
    service_mesh_relation: Relation,
) -> None:
    """Breaking the service-mesh relation clears the labels and deletes the label ConfigMap."""
    state = State(
        leader=True,
        relations=[service_mesh_relation],
        containers=[integration_hub_container],
    )

    integration_hub_ctx.run(integration_hub_ctx.on.relation_broken(service_mesh_relation), state)

    reconcile_charm_labels.assert_called_once()
    assert reconcile_charm_labels.call_args.kwargs["labels"] == {}
    assert reconcile_charm_labels.call_args.kwargs["label_configmap_name"] == LABEL_CONFIGMAP_NAME
    # relation-broken also removes the configmap that tracks previously-applied labels.
    mock_lightkube_client.return_value.delete.assert_called_once()


@patch("managers.s3.S3Manager.verify", return_value=True)
@patch("workload.IntegrationHub.exec", return_value="")
def test_service_mesh_adds_istio_labels_to_spark_properties(
    mock_exec,
    mock_s3_verify,
    integration_hub_ctx: Context[SparkIntegrationHub],
    integration_hub_container: Container,
    spark_service_account_provider_relation: Relation,
    service_mesh_relation: Relation,
) -> None:
    """When the service-mesh relation is present, driver/executor pods get the ambient label."""
    state = State(
        relations=[spark_service_account_provider_relation, service_mesh_relation],
        containers=[integration_hub_container],
        leader=True,
    )

    with (
        patch("managers.k8s.KubernetesManager.__init__", return_value=None),
        patch("managers.k8s.KubernetesManager.trusted", return_value=True),
    ):
        state_out = integration_hub_ctx.run(
            integration_hub_ctx.on.relation_changed(spark_service_account_provider_relation),
            state,
        )

    app_data = state_out.get_relation(spark_service_account_provider_relation.id).local_app_data
    spark_properties = json.loads(app_data.get("spark-properties", "{}"))

    assert (
        spark_properties[f"spark.kubernetes.driver.label.{ISTIO_AMBIENT_LABEL_KEY}"]
        == ISTIO_AMBIENT_LABEL_VALUE
    )
    assert (
        spark_properties[f"spark.kubernetes.executor.label.{ISTIO_AMBIENT_LABEL_KEY}"]
        == ISTIO_AMBIENT_LABEL_VALUE
    )


@patch("managers.s3.S3Manager.verify", return_value=True)
@patch("workload.IntegrationHub.exec", return_value="")
def test_no_service_mesh_relation_omits_istio_labels(
    mock_exec,
    mock_s3_verify,
    integration_hub_ctx: Context[SparkIntegrationHub],
    integration_hub_container: Container,
    spark_service_account_provider_relation: Relation,
) -> None:
    """Without the service-mesh relation, the ambient labels are not added to spark properties."""
    state = State(
        relations=[spark_service_account_provider_relation],
        containers=[integration_hub_container],
        leader=True,
    )

    with (
        patch("managers.k8s.KubernetesManager.__init__", return_value=None),
        patch("managers.k8s.KubernetesManager.trusted", return_value=True),
    ):
        state_out = integration_hub_ctx.run(
            integration_hub_ctx.on.relation_changed(spark_service_account_provider_relation),
            state,
        )

    app_data = state_out.get_relation(spark_service_account_provider_relation.id).local_app_data
    spark_properties = json.loads(app_data.get("spark-properties", "{}"))

    assert f"spark.kubernetes.driver.label.{ISTIO_AMBIENT_LABEL_KEY}" not in spark_properties
    assert f"spark.kubernetes.executor.label.{ISTIO_AMBIENT_LABEL_KEY}" not in spark_properties
