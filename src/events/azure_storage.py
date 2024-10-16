#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Azure Storage Integration related event handlers."""

from charms.data_platform_libs.v0.object_storage import (
    AzureStorageRequires,
    StorageConnectionInfoChangedEvent,
    StorageConnectionInfoGoneEvent,
)
from ops import CharmBase

from common.utils import WithLogging
from core.context import Context
from core.workload import IntegrationHubWorkloadBase
from events.base import BaseEventHandler, compute_status, defer_when_not_ready
from managers.integration_hub import IntegrationHubManager


class AzureStorageEvents(BaseEventHandler, WithLogging):
    """Class implementing Azure Storage Integration event hooks."""

    def __init__(self, charm: CharmBase, context: Context, workload: IntegrationHubWorkloadBase):
        super().__init__(charm, "azure-storage")

        self.charm = charm
        self.context = context
        self.workload = workload

        self.integration_hub = IntegrationHubManager(self.workload)

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

    @compute_status
    @defer_when_not_ready
    def _on_azure_storage_connection_info_changed(self, _: StorageConnectionInfoChangedEvent):
        """Handle the `StorageConnectionInfoChangedEvent` event from Object Storage integrator."""
        self.logger.info("Azure Storage connection info changed")
        self.integration_hub.update(
            self.context.s3,
            self.context.azure_storage,
            self.context.pushgateway,
            self.context.hub_configurations,
            self.context.loki_url,
        )

    @defer_when_not_ready
    def _on_azure_storage_connection_info_gone(self, _: StorageConnectionInfoGoneEvent):
        """Handle the `StorageConnectionInfoGoneEvent` event for Object Storage integrator."""
        self.logger.info("Azure Storage connection info gone")
        self.integration_hub.update(
            self.context.s3,
            None,
            self.context.pushgateway,
            self.context.hub_configurations,
            self.context.loki_url,
        )

        self.charm.unit.status = self.get_app_status(
            self.context.s3, None, self.context.pushgateway
        )
        if self.charm.unit.is_leader():
            self.charm.app.status = self.get_app_status(
                self.context.s3, None, self.context.pushgateway
            )
