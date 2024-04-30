#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""S3 Integration related event handlers."""

from charms.data_platform_libs.v0.s3 import (
    CredentialsChangedEvent,
    CredentialsGoneEvent,
    S3Requirer,
)
from ops import CharmBase

from common.utils import WithLogging
from core.context import Context
from core.workload import IntegrationHubWorkloadBase
from events.base import BaseEventHandler, compute_status
from managers.integration_hub import IntegrationHubManager


class S3Events(BaseEventHandler, WithLogging):
    """Class implementing S3 Integration event hooks."""

    def __init__(self, charm: CharmBase, context: Context, workload: IntegrationHubWorkloadBase):
        super().__init__(charm, "s3")

        self.charm = charm
        self.context = context
        self.workload = workload

        self.integration_hub = IntegrationHubManager(self.workload)

        self.s3_requirer = S3Requirer(self.charm, self.context.s3_endpoint.relation_name)
        self.framework.observe(
            self.s3_requirer.on.credentials_changed, self._on_s3_credential_changed
        )
        self.framework.observe(self.s3_requirer.on.credentials_gone, self._on_s3_credential_gone)

    @compute_status
    def _on_s3_credential_changed(self, _: CredentialsChangedEvent):
        """Handle the `CredentialsChangedEvent` event from S3 integrator."""
        self.logger.info("S3 Credentials changed")
        self.integration_hub.update(
            self.context.s3, self.context.pushgateway, self.context.hub_configurations
        )

    def _on_s3_credential_gone(self, _: CredentialsGoneEvent):
        """Handle the `CredentialsGoneEvent` event for S3 integrator."""
        self.logger.info("S3 Credentials gone")
        self.integration_hub.update(
            None, self.context.pushgateway, self.context.hub_configurations
        )

        self.charm.unit.status = self.get_app_status(None, self.context.pushgateway)
        if self.charm.unit.is_leader():
            self.charm.app.status = self.get_app_status(None, self.context.pushgateway)
