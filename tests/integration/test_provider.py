import json
import logging
import subprocess
from pathlib import Path

import jubilant
import yaml

from .helpers.file import umask_named_temporary_file
from .helpers.integration_hub import (
    deploy_integration_hub_setup,
    deploy_test_charm_setup,
    integration_hub_secret_exists,
)
from .helpers.spark import spark_service_account_exists
from .types import IntegrationTestsCharms

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]
DUMMY_APP_NAME = "app"

REL_NAME_A = "spark-account-a"
REL_NAME_B = "spark-account-b"


def test_build_and_deploy_charms(
    juju: jubilant.Juju,
    hub_charm: Path,
    test_charm: Path,
    charm_versions: IntegrationTestsCharms,
    namespace: str,
) -> None:
    """Build the charm-under-test and deploy it together with related charms.

    Assert on the unit status before any relations/configurations take place.
    """
    deploy_integration_hub_setup(
        juju=juju,
        hub_charm=hub_charm,
        charm_versions=charm_versions,
        trust=True,
    )
    deploy_test_charm_setup(
        juju=juju, test_charm=test_charm, spark_workload_namespace=namespace, integrate=False
    )
    juju.wait(jubilant.all_active)


def test_relate_charms(juju: jubilant.Juju, namespace: str) -> None:
    configuration_parameters = {"namespace": namespace}

    logger.info(f"Setting config for test application charm: {configuration_parameters}...")
    juju.config(DUMMY_APP_NAME, configuration_parameters)
    juju.wait(
        lambda status: jubilant.all_active(status) and jubilant.all_agents_idle(status), delay=5
    )

    logger.info("Integrating integration hub with test application for service account sa1")
    juju.integrate(APP_NAME, f"{DUMMY_APP_NAME}:{REL_NAME_A}")
    juju.wait(
        lambda status: jubilant.all_active(status) and jubilant.all_agents_idle(status), delay=5
    )

    assert spark_service_account_exists(namespace, "sa1")
    assert integration_hub_secret_exists(
        workload_namespace=namespace, workload_service_account="sa1"
    )

    logger.info("Enable autoscaling...")
    juju.config(APP_NAME, {"enable-dynamic-allocation": "true"})
    juju.wait(
        lambda status: jubilant.all_active(status) and jubilant.all_agents_idle(status), delay=5
    )

    # The added spark properties be reflected on the requirer charm
    task = juju.run(f"{DUMMY_APP_NAME}/0", "get-properties-sa1")
    assert task.return_code == 0
    assert "spark-properties" in task.results
    properties = task.results["spark-properties"]
    assert "spark.dynamicAllocation.enabled" in json.loads(properties)

    task = juju.run(f"{DUMMY_APP_NAME}/0", "get-resource-manifest-sa1")
    assert task.return_code == 0
    assert "resource-manifest" in task.results
    manifest = task.results["resource-manifest"]
    assert manifest is not None
    assert manifest.strip() != ""

    # Add a new relation between dummy application charm and integration hub
    logger.info("Integrating integration hub with test application for service account sa2")
    juju.integrate(APP_NAME, f"{DUMMY_APP_NAME}:{REL_NAME_B}")

    juju.wait(
        lambda status: jubilant.all_active(status) and jubilant.all_agents_idle(status), delay=5
    )

    assert spark_service_account_exists(namespace, "sa2")
    assert integration_hub_secret_exists(
        workload_namespace=namespace, workload_service_account="sa2"
    )

    # The added spark property be reflected on the requirer charm
    task = juju.run(f"{DUMMY_APP_NAME}/0", "get-properties-sa2")
    assert task.return_code == 0
    assert "spark-properties" in task.results
    properties = task.results["spark-properties"]
    assert "spark.dynamicAllocation.enabled" in json.loads(properties)

    task = juju.run(f"{DUMMY_APP_NAME}/0", "get-resource-manifest-sa2")
    assert task.return_code == 0
    assert "resource-manifest" in task.results
    manifest = task.results["resource-manifest"]
    assert manifest is not None
    assert manifest.strip() != ""


def test_remove_relation(juju: jubilant.Juju, namespace: str) -> None:
    logger.info(
        "Removing relation between integration hub and test application for service account sa1"
    )
    juju.remove_relation(APP_NAME, f"{DUMMY_APP_NAME}:{REL_NAME_A}")

    juju.wait(
        lambda status: jubilant.all_active(status) and jubilant.all_agents_idle(status), delay=5
    )
    assert not spark_service_account_exists(namespace, "sa1")

    logger.info(
        "Removing relation between integration hub and test application for service account sa2"
    )
    juju.remove_relation(APP_NAME, f"{DUMMY_APP_NAME}:{REL_NAME_B}")

    juju.wait(
        lambda status: jubilant.all_active(status) and jubilant.all_agents_idle(status), delay=5
    )
    assert not spark_service_account_exists(namespace, "sa2")


def test_skip_creation_of_resources(juju: jubilant.Juju, namespace: str) -> None:
    """Test the behavior of passing skip-creation flag in the spark-service-account relation."""
    configuration_parameters = {"skip-creation": "true", "namespace": namespace}
    juju.config(DUMMY_APP_NAME, configuration_parameters)
    juju.wait(
        lambda status: jubilant.all_active(status) and jubilant.all_agents_idle(status), delay=5
    )

    logger.info("Integrating integration hub with test application for service account sa1")
    juju.integrate(APP_NAME, f"{DUMMY_APP_NAME}:{REL_NAME_A}")
    juju.wait(
        lambda status: jubilant.all_active(status) and jubilant.all_agents_idle(status), delay=5
    )

    assert not spark_service_account_exists(namespace, "sa1")

    # Add a spark property via configuration action of integration hub
    logger.info("Enable autoscaling...")
    juju.config(APP_NAME, {"enable-dynamic-allocation": "true"})
    juju.wait(
        lambda status: jubilant.all_active(status) and jubilant.all_agents_idle(status), delay=5
    )

    # The added spark property be reflected on the requirer charm
    task = juju.run(f"{DUMMY_APP_NAME}/0", "get-properties-sa1")
    assert task.return_code == 0
    assert "spark-properties" in task.results
    properties = task.results["spark-properties"]
    assert "spark.dynamicAllocation.enabled" in json.loads(properties)

    task = juju.run(f"{DUMMY_APP_NAME}/0", "get-resource-manifest-sa1")
    assert task.return_code == 0
    assert "resource-manifest" in task.results
    manifest = task.results["resource-manifest"]
    assert manifest is not None

    # Write the manifest to a temporary file
    with umask_named_temporary_file(mode="w+", prefix="manifest-", suffix=".yaml") as tmp:
        tmp.write(manifest)
        tmp.flush()

        # Now try applying the manifest file
        apply_result = subprocess.run(
            ["kubectl", "apply", "-f", tmp.name],
            check=True,
            capture_output=True,
            text=True,
        )
        assert apply_result.returncode == 0

    # Now the service account should have been created there
    assert spark_service_account_exists(namespace, "sa1")

    # Once the resource manifest is applied, the service account config should be
    # readable by `spark8t.cli.service_account_registry get-config` command
    command = [
        "python3",
        "-m",
        "spark8t.cli.service_account_registry",
        "get-config",
        "--username=sa1",
        f"--namespace={namespace}",
    ]
    print(" ".join(command))

    get_config_process = subprocess.run(command, check=True, capture_output=True)
    assert get_config_process.returncode == 0

    actual_config_lines = get_config_process.stdout.decode().strip().splitlines()
    expected_config_lines = [
        "spark.dynamicAllocation.enabled=true",
        "spark.dynamicAllocation.shuffleTracking.enabled=true",
        "spark.dynamicAllocation.minExecutors=1",
    ]

    # Assert that the config injected by integration hub is there
    assert all(line in actual_config_lines for line in expected_config_lines)
