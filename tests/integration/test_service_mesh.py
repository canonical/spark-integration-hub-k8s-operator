#!/usr/bin/env python3
# Copyright 2026 Canonical Limited
# See LICENSE file for licensing details.

import jubilant

from .types import IntegrationTestsCharms, S3Info
from .utils.integration_hub import deploy_integration_hub_setup


def test_deploy_integration_hub(
    juju: jubilant.Juju,
    hub_charm: str,
    charm_versions: IntegrationTestsCharms,
    s3_credentials: S3Info,
):
    """Test deploying the integration hub with S3 storage integration."""
    deploy_integration_hub_setup(
        juju=juju,
        hub_charm=hub_charm,
        charm_versions=charm_versions,
        s3_credentials=s3_credentials,
        trust=True,
    )
    juju.wait(jubilant.all_active)


def test_run_spark_job_before_meshing():
    pass


def test_access_spark_workloads_from_unmeshed_pod_before_meshing():
    pass


def test_enable_service_mesh():
    pass


def test_access_spark_workloads_from_unmeshed_pod_after_meshing():
    pass


def test_access_spark_workloads_from_meshed_pod_but_unauthorized_after_meshing():
    pass


def test_run_spark_job_after_meshing():
    pass


def test_observability_with_ambient_mesh():
    pass


def test_deploy_and_integrate_client_app():
    pass


def test_access_spark_workload_from_client_app_after_meshing():
    pass


def test_disable_service_mesh():
    pass


def test_run_spark_job_after_unmeshing():
    pass


def test_access_spark_workloads_from_unmeshed_pod_after_unmeshing():
    pass
