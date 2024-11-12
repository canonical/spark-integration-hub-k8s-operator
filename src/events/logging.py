#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Logging Integration related event handlers."""

from charms.loki_k8s.v1.loki_push_api import LogForwarder
from ops import CharmBase, RelationBrokenEvent, RelationChangedEvent

from common.utils import WithLogging
from constants import LOGGING_RELATION_NAME
from core.context import Context
from core.workload import IntegrationHubWorkloadBase
from events.base import BaseEventHandler, compute_status, defer_when_not_ready
from managers.integration_hub import IntegrationHubManager


class LoggingEvents(BaseEventHandler, WithLogging):
    """Class implementing logging integration event hooks."""

    def __init__(self, charm: CharmBase, context: Context, workload: IntegrationHubWorkloadBase):
        super().__init__(charm, "Logging")

        self.charm = charm
        self.context = context
        self.workload = workload

        # Log forwarding to Loki
        self.charm._logging = LogForwarder(charm=self.charm, relation_name=LOGGING_RELATION_NAME)
        self.framework.observe(
            self.charm.on[LOGGING_RELATION_NAME].relation_changed, self._on_update_loki_url
        )
        self.framework.observe(
            self.charm.on[LOGGING_RELATION_NAME].relation_broken, self._on_remove_loki_url
        )

        # define integration hub manager
        self.integration_hub = IntegrationHubManager(self.workload)

    @compute_status
    @defer_when_not_ready
    def _on_update_loki_url(self, _: RelationChangedEvent):
        """Handle the `LoggingChangedEvent` event."""
        self.logger.info("Logging changed")
        self.integration_hub.update(
            self.context.s3,
            self.context.azure_storage,
            self.context.pushgateway,
            self.context.hub_configurations,
            self.context.loki_url,
        )

    @defer_when_not_ready
    def _on_remove_loki_url(self, _: RelationBrokenEvent):
        """Handle the `LoggingBrokenEvent` event."""
        self.logger.info("Logging removed")
        self.integration_hub.update(
            self.context.s3,
            self.context.azure_storage,
            self.context.pushgateway,
            self.context.hub_configurations,
            None,
        )
