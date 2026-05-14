#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""S3 Integration related event handlers."""

from __future__ import annotations

from typing import TYPE_CHECKING

from object_storage import (
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


class S3Events(BaseEventHandler, WithLogging):
    """Class implementing S3 Integration event hooks."""

    def __init__(
        self, charm: SparkIntegrationHub, context: Context, workload: IntegrationHubWorkloadBase
    ) -> None:
        super().__init__(charm, "s3")

        self.charm = charm
        self.context = context
        self.workload = workload

        self.integration_hub = IntegrationHubManager(
            self.workload, self.context, self.charm.config
        )
        self.s3_requirer = self.context.s3_requirer
        self.framework.observe(
            self.s3_requirer.on.storage_connection_info_changed, self._on_s3_credential_changed
        )
        self.framework.observe(
            self.s3_requirer.on.storage_connection_info_gone, self._on_s3_credential_gone
        )

    @defer_when_not_ready
    def _on_s3_credential_changed(self, _: StorageConnectionInfoChangedEvent) -> None:
        """Handle the `StorageConnectionInfoChangedEvent` event from S3 integrator."""
        self.logger.info("S3 Credentials changed")
        self.integration_hub.update()

    @defer_when_not_ready
    def _on_s3_credential_gone(self, _: StorageConnectionInfoGoneEvent) -> None:
        """Handle the `StorageConnectionInfoGoneEvent` event for S3 integrator."""
        self.logger.info("S3 Credentials gone")
        self.integration_hub.update(set_s3_none=True)
