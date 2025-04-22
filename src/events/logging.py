#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Logging Integration related event handlers."""

from __future__ import annotations

from typing import TYPE_CHECKING

from charms.loki_k8s.v1.loki_push_api import LogForwarder
from ops import RelationBrokenEvent, RelationChangedEvent

from common.utils import WithLogging
from constants import LOGGING_RELATION_NAME
from core.context import Context
from core.workload import IntegrationHubWorkloadBase
from events.base import BaseEventHandler, compute_status, defer_when_not_ready
from managers.integration_hub import IntegrationHubManager

if TYPE_CHECKING:
    from charm import SparkIntegrationHub


class LoggingEvents(BaseEventHandler, WithLogging):
    """Class implementing logging integration event hooks."""

    def __init__(
        self, charm: SparkIntegrationHub, context: Context, workload: IntegrationHubWorkloadBase
    ):
        super().__init__(charm, "Logging")

        self.charm = charm
        self.context = context
        self.workload = workload

        # Log forwarding to Loki
        self.logging = LogForwarder(charm=self.charm, relation_name=LOGGING_RELATION_NAME)
        self.framework.observe(
            self.charm.on[LOGGING_RELATION_NAME].relation_changed, self._on_update_loki_url
        )
        self.framework.observe(
            self.charm.on[LOGGING_RELATION_NAME].relation_broken, self._on_remove_loki_url
        )

        # define integration hub manager
        self.integration_hub = IntegrationHubManager(self.workload, self.context)

    @compute_status
    @defer_when_not_ready
    def _on_update_loki_url(self, _: RelationChangedEvent) -> None:
        """Handle the `LoggingChangedEvent` event."""
        self.logger.info("Logging changed")
        self.integration_hub.update()

    @defer_when_not_ready
    def _on_remove_loki_url(self, _: RelationBrokenEvent) -> None:
        """Handle the `LoggingBrokenEvent` event."""
        self.logger.info("Logging removed")
        self.integration_hub.update(set_loki_url_none=True)
