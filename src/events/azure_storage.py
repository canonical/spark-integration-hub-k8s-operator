#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Azure Storage Integration related event handlers."""

from __future__ import annotations

from typing import TYPE_CHECKING

from charms.data_platform_libs.v0.object_storage import (
    AzureStorageRequires,
    StorageConnectionInfoChangedEvent,
    StorageConnectionInfoGoneEvent,
)

from common.utils import WithLogging
from core.context import Context
from core.workload import IntegrationHubWorkloadBase
from events.base import BaseEventHandler, defer_when_not_ready
from managers.integration_hub import IntegrationHubManager

if TYPE_CHECKING:
    from charm import SparkIntegrationHub


class AzureStorageEvents(BaseEventHandler, WithLogging):
    """Class implementing Azure Storage Integration event hooks."""

    def __init__(
        self, charm: SparkIntegrationHub, context: Context, workload: IntegrationHubWorkloadBase
    ) -> None:
        super().__init__(charm, "azure-storage")

        self.charm = charm
        self.context = context
        self.workload = workload

        self.integration_hub = IntegrationHubManager(
            self.workload, self.context, self.charm.config
        )

        self.azure_storage_requirer = AzureStorageRequires(
            self.charm, self.context.azure_storage_endpoint.relation_name
        )
        self.framework.observe(
            self.azure_storage_requirer.on.storage_connection_info_changed,
            self._on_azure_storage_connection_info_changed,
        )
        self.framework.observe(
            self.azure_storage_requirer.on.storage_connection_info_gone,
            self._on_azure_storage_connection_info_gone,
        )

    @defer_when_not_ready
    def _on_azure_storage_connection_info_changed(
        self, _: StorageConnectionInfoChangedEvent
    ) -> None:
        """Handle the `StorageConnectionInfoChangedEvent` event from Object Storage integrator."""
        self.logger.info("Azure Storage connection info changed")
        self.integration_hub.update()

    @defer_when_not_ready
    def _on_azure_storage_connection_info_gone(self, _: StorageConnectionInfoGoneEvent) -> None:
        """Handle the `StorageConnectionInfoGoneEvent` event for Object Storage integrator."""
        self.logger.info("Azure Storage connection info gone")
        self.integration_hub.update(set_azure_storage_none=True)
