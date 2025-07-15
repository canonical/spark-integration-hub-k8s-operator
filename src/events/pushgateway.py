#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Prometheus PushGateway related event handlers."""

from __future__ import annotations

from typing import TYPE_CHECKING

from charms.prometheus_pushgateway_k8s.v0.pushgateway import PrometheusPushgatewayRequirer
from ops import RelationBrokenEvent, RelationChangedEvent

from common.utils import WithLogging
from core.context import PUSHGATEWAY, Context
from core.workload import IntegrationHubWorkloadBase
from events.base import BaseEventHandler, defer_when_not_ready
from managers.integration_hub import IntegrationHubManager

if TYPE_CHECKING:
    from charm import SparkIntegrationHub


class PushgatewayEvents(BaseEventHandler, WithLogging):
    """Class implementing PushGateway event hooks."""

    def __init__(
        self, charm: SparkIntegrationHub, context: Context, workload: IntegrationHubWorkloadBase
    ) -> None:
        super().__init__(charm, PUSHGATEWAY)

        self.charm = charm
        self.context = context
        self.workload = workload

        self.integration_hub = IntegrationHubManager(
            self.workload, self.context, self.charm.config
        )

        self.pushgateway = PrometheusPushgatewayRequirer(self.charm, PUSHGATEWAY)

        self.framework.observe(
            self.charm.on[PUSHGATEWAY].relation_changed, self._on_pushgateway_changed
        )
        self.framework.observe(
            self.charm.on[PUSHGATEWAY].relation_broken, self._on_pushgateway_gone
        )

    @defer_when_not_ready
    def _on_pushgateway_changed(self, _: RelationChangedEvent) -> None:
        """Handle the `RelationChanged` event from the PushGateway."""
        self.logger.info("PushGateway relation changed")
        self.logger.info(f"PushGateway ready: {self.pushgateway.is_ready()}")
        if self.pushgateway.is_ready():
            self.integration_hub.update()

    @defer_when_not_ready
    def _on_pushgateway_gone(self, _: RelationBrokenEvent) -> None:
        """Handle the `RelationBroken` event for PushGateway."""
        self.logger.info("PushGateway relation broken")
        self.integration_hub.update(set_pushgateway_none=True)
