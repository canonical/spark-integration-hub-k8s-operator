#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Spark Integration Hub workload related event handlers."""

from ops.charm import CharmBase

from common.utils import WithLogging
from constants import INTEGRATION_HUB_LABEL, PEER
from core.context import Context
from core.workload import IntegrationHubWorkloadBase
from events.base import BaseEventHandler, compute_status, defer_when_not_ready
from managers.integration_hub import IntegrationHubManager


class IntegrationHubEvents(BaseEventHandler, WithLogging):
    """Class implementing Spark Integration Hub event hooks."""

    def __init__(self, charm: CharmBase, context: Context, workload: IntegrationHubWorkloadBase):
        super().__init__(charm, "integration-hub")
        self.charm = charm
        self.context = context
        self.workload = workload

        self.integration_hub = IntegrationHubManager(self.workload)

        self.framework.observe(
            self.charm.on.integration_hub_pebble_ready,
            self._on_integration_hub_pebble_ready,
        )
        self.framework.observe(self.charm.on.update_status, self._update_event)
        self.framework.observe(self.charm.on.install, self._update_event)
        self.framework.observe(self.charm.on.stop, self._remove_resources)
        self.framework.observe(
            self.charm.on[PEER].relation_changed, self._on_peer_relation_changed
        )

    def _remove_resources(self, _):
        """Handle the stop event."""
        self.integration_hub.workload.exec(
            f"kubectl delete secret -l {INTEGRATION_HUB_LABEL} --all-namespaces"
        )

    @compute_status
    @defer_when_not_ready
    def _on_integration_hub_pebble_ready(self, _):
        """Handle on Pebble ready event."""
        self.integration_hub.update(
            self.context.s3, self.context.azure_storage, self.context.pushgateway, self.context.hub_configurations
        )

    @compute_status
    @defer_when_not_ready
    def _on_peer_relation_changed(self, _):
        """Handle on PEER relation changed event."""
        self.integration_hub.update(
            self.context.s3, self.context.azure_storage, self.context.pushgateway, self.context.hub_configurations
        )

    @compute_status
    def _update_event(self, _):
        pass
