#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Module containing all business logic related to the workload."""

import json
from typing import cast

import ops.pebble
from ops.model import Container

from common.k8s import K8sWorkload
from common.utils import WithLogging
from core.domain import User
from core.workload import IntegrationHubPaths, IntegrationHubWorkloadBase


class IntegrationHub(IntegrationHubWorkloadBase, K8sWorkload, WithLogging):
    """Class representing Workload implementation for the Integration Hub charm on K8s."""

    CONTAINER = "integration-hub"
    CONTAINER_LAYER = "integration-hub"

    INTEGRATION_HUB_SERVICE = "integration-hub"

    CONFS_PATH = "/etc/hub/conf"
    ENV_FILE = "/etc/hub/environment"

    def __init__(self, container: Container, user: User):
        self.container = container
        self.user = user

        self.paths = IntegrationHubPaths(conf_path=self.CONFS_PATH, keytool="keytool")
        self._envs: dict[str, str] | None = None

    @property
    def envs(self):
        """Return current environment."""
        if self._envs is not None:
            return self._envs

        self._envs = self.from_env(self.read(self.ENV_FILE)) if self.exists(self.ENV_FILE) else {}

        return self._envs

    @property
    def _spark_integration_hub_layer(self) -> dict:
        """Return a dictionary representing a Pebble layer."""
        layer = {
            "summary": "spark integration hub layer",
            "description": "pebble config layer for spark integration hub",
            "services": {
                self.INTEGRATION_HUB_SERVICE: {
                    "environment": self.envs,
                }
            },
        }
        self.logger.info(f"Layer: {json.dumps(layer)}")
        return layer

    def start(self) -> None:
        """Execute business-logic for starting the workload."""
        layer: dict = dict(self.container.get_plan().to_dict())

        layer["services"][self.INTEGRATION_HUB_SERVICE] = (
            layer["services"][self.INTEGRATION_HUB_SERVICE]
            | self._spark_integration_hub_layer["services"][self.INTEGRATION_HUB_SERVICE]
        )

        # Temporal fix to restart the container when the watch process fails
        layer["services"][self.INTEGRATION_HUB_SERVICE]["on-failure"] = "restart"

        self.container.add_layer(
            self.CONTAINER_LAYER, cast(ops.pebble.LayerDict, layer), combine=True
        )

        if not self.exists(str(self.paths.spark_properties)):
            self.logger.error(f"{self.paths.spark_properties} not found")
            raise FileNotFoundError(self.paths.spark_properties)

        # Push an updated layer with the new config
        self.container.restart(self.INTEGRATION_HUB_SERVICE)

    def stop(self) -> None:
        """Execute business-logic for stopping the workload."""
        self.container.stop(self.INTEGRATION_HUB_SERVICE)

    def ready(self) -> bool:
        """Check whether the service is ready to be used."""
        return self.container.can_connect()

    def active(self) -> bool:
        """Return the health of the service."""
        try:
            service = self.container.get_service(self.INTEGRATION_HUB_SERVICE)
        except ops.pebble.ConnectionError:
            self.logger.debug(f"Service {self.INTEGRATION_HUB_SERVICE} not running")
            return False

        return service.is_running()

    def set_environment(self, env: dict[str, str | None]) -> None:
        """Set environment for workload."""
        merged_envs = self.envs | env

        self._envs = {k: v for k, v in merged_envs.items() if v is not None}

        self.write("\n".join(self.to_env(self.envs)), self.ENV_FILE)

    def create_service_account(self, namespace: str, username: str) -> None:
        """Create service account in given namespace."""
        try:
            self.exec(
                f"python3 -m spark8t.cli.service_account_registry create --username={username} --namespace={namespace}"
            )
        except Exception as e:
            self.logger.error(e)
            raise RuntimeError(
                f"Impossible to create service account: {username} in namespace: {namespace}"
            )

    def delete_service_account(self, namespace: str, username: str):
        """Delete service account in given namespace."""
        try:
            self.exec(
                f"python3 -m spark8t.cli.service_account_registry delete --username={username} --namespace={namespace}"
            )
        except Exception as e:
            self.logger.error(e)
            raise RuntimeError(
                f"Failed to delete service account: {username} in namespace: {namespace}"
            )
