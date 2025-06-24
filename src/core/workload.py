#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Implementation and blue-print for Spark Integration Hub workloads."""

from abc import abstractmethod
from pathlib import Path

from common.workload import AbstractWorkload
from core.domain import User


class IntegrationHubPaths:
    """Object to store common paths for Kafka."""

    def __init__(self, conf_path: Path | str, keytool: str):
        self.conf_path = conf_path if isinstance(conf_path, Path) else Path(conf_path)
        self.keytool = keytool

    @property
    def spark_properties(self) -> Path:
        """Return the path of the spark-properties file."""
        return self.conf_path / "spark-properties.conf"


class IntegrationHubWorkloadBase(AbstractWorkload):
    """Base interface for common workload operations."""

    paths: IntegrationHubPaths
    user: User

    def restart(self) -> None:
        """Restarts the workload service."""
        self.stop()
        self.start()

    @abstractmethod
    def set_environment(self, env: dict[str, str | None]):
        """Set the environment."""
        ...

    @abstractmethod
    def active(self) -> bool:
        """Checks that the workload is active."""
        ...

    @abstractmethod
    def ready(self) -> bool:
        """Checks that the container/snap is ready."""
        ...

    @abstractmethod
    def create_service_account(self, namespace: str, username: str) -> None:
        """Create the service account in the given namespace."""
        ...

    @abstractmethod
    def delete_service_account(self, namespace: str, username: str) -> None:
        """Delete the service account in the given namespace."""
        ...
