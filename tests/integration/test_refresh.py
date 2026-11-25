# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

from pathlib import Path

import jubilant
import lightkube
import yaml

from .helpers.spark import (
    assert_spark_job_successful,
    cleanup_workload_pods,
    run_spark_job,
    setup_spark_job,
)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]


def _wait_for_hub(juju: jubilant.Juju, base_version: str) -> None:
    juju.wait(
        lambda status: jubilant.all_active(status, APP_NAME) and jubilant.all_agents_idle(status)
    )

    base = juju.status().apps[APP_NAME].base
    assert base is not None
    assert base.name == "ubuntu"
    assert base.channel == base_version


def _run_spark_job(
    lightkube_client: lightkube.Client, namespace: str, service_account: str
) -> None:
    setup_spark_job(namespace=namespace, service_account=service_account)
    try:
        run_spark_job(namespace=namespace, service_account=service_account)
        assert_spark_job_successful(lightkube_client, namespace=namespace)
    finally:
        cleanup_workload_pods(lightkube_client, namespace=namespace)


def test_deploy(
    juju: jubilant.Juju,
    service_account: tuple[str, str],
    lightkube_client: lightkube.Client,
) -> None:
    juju.deploy(
        APP_NAME,
        app=APP_NAME,
        channel="3/stable",
        # FIXME: Once 26.04 is released to stable, we should replace those 22.04 values
        base="ubuntu@22.04",
        num_units=1,
        trust=True,
    )
    _wait_for_hub(juju, "22.04")
    service_account_name, namespace = service_account
    _run_spark_job(lightkube_client, namespace, service_account_name)


def test_refresh_from_previous_stable(
    juju: jubilant.Juju,
    hub_charm: Path,
    service_account: tuple[str, str],
    lightkube_client: lightkube.Client,
) -> None:
    """Test refreshing from 3/stable to the current charm."""
    image_version = METADATA["resources"]["integration-hub-image"]["upstream-source"]
    juju.refresh(
        APP_NAME,
        path=hub_charm,
        base="ubuntu@26.04",
        resources={"integration-hub-image": image_version},
        trust=True,
    )
    _wait_for_hub(juju, "26.04")

    service_account_name, namespace = service_account
    _run_spark_job(lightkube_client, namespace, service_account_name)
