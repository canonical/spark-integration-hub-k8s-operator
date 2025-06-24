#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Kubernetes manager."""

from lightkube.core.client import Client
from lightkube.core.exceptions import ApiError
from lightkube.resources.core_v1 import ServiceAccount

from common.utils import WithLogging


class KubernetesManager(WithLogging):
    """Class exposing business logic for interacting with Kubernetes."""

    def __init__(self, app_name: str):
        self.app_name = app_name
        self.client = Client(field_manager=app_name)

    def trusted(self) -> bool:
        """Check if the charm is trusted."""
        try:
            _ = self.client.list(ServiceAccount, namespace="*")
            return True
        except ApiError:
            return False
