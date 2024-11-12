#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Prometheus PushGateway related event handlers."""


from charms.prometheus_pushgateway_k8s.v0.pushgateway import PrometheusPushgatewayRequirer
from ops import CharmBase, RelationBrokenEvent, RelationChangedEvent

from common.utils import WithLogging
from core.context import PUSHGATEWAY, Context
from core.workload import IntegrationHubWorkloadBase
from events.base import BaseEventHandler, compute_status, defer_when_not_ready
from managers.integration_hub import IntegrationHubManager


class PushgatewayEvents(BaseEventHandler, WithLogging):
    """Class implementing PushGateway event hooks."""

    def __init__(self, charm: CharmBase, context: Context, workload: IntegrationHubWorkloadBase):
        super().__init__(charm, PUSHGATEWAY)

        self.charm = charm
        self.context = context
        self.workload = workload

        self.integration_hub = IntegrationHubManager(self.workload)

        self.pushgateway = PrometheusPushgatewayRequirer(self.charm, PUSHGATEWAY)

        self.framework.observe(
            self.charm.on[PUSHGATEWAY].relation_changed, self._on_pushgateway_changed
        )
        self.framework.observe(
            self.charm.on[PUSHGATEWAY].relation_broken, self._on_pushgateway_gone
        )

    @compute_status
    @defer_when_not_ready
    def _on_pushgateway_changed(self, _: RelationChangedEvent):
        """Handle the `RelationChanged` event from the PushGateway."""
        self.logger.info("PushGateway relation changed")
        self.logger.info(f"PushGateway ready: {self.pushgateway.is_ready()}")
        if self.pushgateway.is_ready():
            self.integration_hub.update(
                self.context.s3,
                self.context.azure_storage,
                self.context.pushgateway,
                self.context.hub_configurations,
                self.context.loki_url,
            )

    @defer_when_not_ready
    def _on_pushgateway_gone(self, _: RelationBrokenEvent):
        """Handle the `RelationBroken` event for PushGateway."""
        self.logger.info("PushGateway relation broken")
        self.integration_hub.update(
            self.context.s3,
            self.context.azure_storage,
            None,
            self.context.hub_configurations,
            self.context.loki_url,
        )

        self.charm.unit.status = self.get_app_status(
            self.context.s3, self.context.azure_storage, None
        )
        if self.charm.unit.is_leader():
            self.charm.app.status = self.get_app_status(
                self.context.s3, self.context.azure_storage, None
            )
