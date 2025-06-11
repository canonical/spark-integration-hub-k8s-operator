#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Spark Service accounts related event handlers."""

from __future__ import annotations

from typing import TYPE_CHECKING

import yaml
from charms.spark_integration_hub_k8s.v0.spark_service_account import (
    ServiceAccountReleasedEvent,
    ServiceAccountRequestedEvent,
    SparkServiceAccountProvider,
)

from common.utils import WithLogging
from constants import INTEGRATION_HUB_REL
from core.context import Context
from core.workload import IntegrationHubWorkloadBase
from events.base import BaseEventHandler, defer_when_not_ready
from managers.integration_hub import IntegrationHubManager

if TYPE_CHECKING:
    from charm import SparkIntegrationHub


class SparkServiceAccountProviderEvents(BaseEventHandler, WithLogging):
    """Class implementing Spark Service Account Integration event hooks."""

    def __init__(
        self, charm: SparkIntegrationHub, context: Context, workload: IntegrationHubWorkloadBase
    ) -> None:
        super().__init__(charm, "service-account")

        self.charm = charm
        self.context = context
        self.workload = workload

        self.sa = SparkServiceAccountProvider(self.charm, INTEGRATION_HUB_REL)
        self.integration_hub = IntegrationHubManager(
            self.workload, self.context, self.charm.config
        )

        self.framework.observe(self.sa.on.account_requested, self._on_service_account_requested)
        self.framework.observe(self.sa.on.account_released, self._on_service_account_released)

    @defer_when_not_ready
    def _on_service_account_requested(self, event: ServiceAccountRequestedEvent) -> None:
        """Handle the `ServiceAccountRequested` event for the Spark Integration hub."""
        self.logger.info("Service account requested.")

        if not self.charm.unit.is_leader():
            return

        if not event.service_account:
            return

        relation_id = event.relation.id
        namespace, username = event.service_account.split(":")
        skip_creation = event.skip_creation
        self.logger.debug(f"Desired service account name: {username} in namespace: {namespace}")

        if not skip_creation:
            self.workload.create_service_account(namespace=namespace, username=username)

        self.sa.set_service_account(relation_id, event.service_account)  # type: ignore

        # TODO: Add logic for generation of resource manifest
        self.sa.set_resource_manifest(relation_id, resource_manifest=yaml.dump({}))

        self.integration_hub.update()

    @defer_when_not_ready
    def _on_service_account_released(self, event: ServiceAccountReleasedEvent) -> None:
        """Handle the `ServiceAccountReleased` event for the Spark Integration hub."""
        self.logger.info("Service account released.")

        if not self.charm.unit.is_leader():
            return

        if not event.service_account:
            return

        namespace, username = event.service_account.split(":")
        skip_creation = event.skip_creation

        self.logger.debug(
            f"The service account name: {username} in namespace: {namespace} should be deleted"
        )

        if not skip_creation:
            self.workload.delete_service_account(namespace=namespace, username=username)
