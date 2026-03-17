#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Kubernetes manager."""

import fnmatch
import re
from functools import cached_property

from lightkube.core.client import Client, LabelValue
from lightkube.core.exceptions import ApiError
from lightkube.models import authorization_v1
from lightkube.resources.authorization_v1 import SelfSubjectAccessReview
from lightkube.resources.core_v1 import Namespace, Secret

from common.utils import WithLogging
from constants import INTEGRATION_HUB_LABEL


class KubernetesManager(WithLogging):
    """Class exposing business logic for interacting with Kubernetes."""

    def __init__(self, app_name: str):
        self.app_name = app_name

    @cached_property
    def client(self) -> Client:
        """Return the lightkube client."""
        return Client(field_manager=self.app_name)

    def trusted(self) -> bool:
        """Check whether application is trusted."""
        try:
            return getattr(
                self.client.create(
                    SelfSubjectAccessReview(
                        spec=authorization_v1.SelfSubjectAccessReviewSpec(
                            resourceAttributes=authorization_v1.ResourceAttributes(
                                name=self.app_name,
                                namespace="test",
                                resource="statefulset",
                                verb="patch",
                            )
                        )
                    )
                ).status,
                "allowed",
                False,
            )
        except ApiError:
            return False

    @staticmethod
    def get_allowed_namespaces(allowlist: list[str]) -> set[str]:
        """Build shell-style patterns from allowlist."""
        allowed_namespaces = set()
        for entry in allowlist:
            ns, _, _ = entry.partition(":")
            allowed_namespaces.add(fnmatch.translate(ns))

        return allowed_namespaces

    @staticmethod
    def is_allowed(namespace: str, patterns: set[str]) -> bool:
        """Compare a service account against a list of shell-style patterns."""
        return any(re.match(namespace_patterns, namespace) for namespace_patterns in patterns)

    def delete_secrets(self, monitor_service_accounts: list[str]) -> None:
        """Delete a secret."""
        try:
            namespaces = self.client.list(Namespace)

            patterns = self.get_allowed_namespaces(monitor_service_accounts)
            label: dict[str, LabelValue] = {
                INTEGRATION_HUB_LABEL.split("=")[0]: INTEGRATION_HUB_LABEL.split("=")[1]
            }
            for ns in namespaces:
                namespace_name = ns.metadata.name if ns.metadata and ns.metadata.name else ""

                if self.is_allowed(namespace_name, patterns) and namespace_name:
                    self.logger.info(f"Deleting secrets in namespace {namespace_name}...")

                    for secret in self.client.list(Secret, namespace=namespace_name, labels=label):
                        secret_name = (
                            secret.metadata.name
                            if secret.metadata and secret.metadata.name
                            else ""
                        )
                        self.logger.info(
                            f"Deleting secret {secret_name} in namespace {namespace_name}..."
                        )
                        self.client.delete(
                            Secret,
                            name=secret_name,
                            namespace=namespace_name,
                        ) if secret_name else self.logger.warning(
                            f"Secret in namespace {namespace_name} has no name, skipping deletion."
                        )

        except ApiError as e:
            self.logger.error(f"Failed to delete secrets associated to integration-hub: {e}")
