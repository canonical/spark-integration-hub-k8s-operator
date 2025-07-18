#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Spark Integration Hub workload related event handlers."""

from __future__ import annotations

from typing import TYPE_CHECKING

from ops import (
    ConfigChangedEvent,
    PebbleReadyEvent,
    StopEvent,
)
from ops.pebble import ExecError

from common.utils import WithLogging
from constants import INTEGRATION_HUB_LABEL
from core.context import Context
from core.workload import IntegrationHubWorkloadBase
from events.base import BaseEventHandler, defer_when_not_ready
from managers.integration_hub import IntegrationHubManager

if TYPE_CHECKING:
    from charm import SparkIntegrationHub


class IntegrationHubEvents(BaseEventHandler, WithLogging):
    """Class implementing Spark Integration Hub event hooks."""

    def __init__(
        self, charm: SparkIntegrationHub, context: Context, workload: IntegrationHubWorkloadBase
    ) -> None:
        super().__init__(charm, "integration-hub")
        self.charm = charm
        self.context = context
        self.workload = workload

        self.integration_hub = IntegrationHubManager(
            self.workload, self.context, self.charm.config
        )

        self.framework.observe(
            self.charm.on.integration_hub_pebble_ready,
            self._on_integration_hub_pebble_ready,
        )
        self.framework.observe(self.charm.on.stop, self._remove_resources)
        self.framework.observe(self.charm.on.config_changed, self._on_config_changed)

    def _remove_resources(self, _: StopEvent) -> None:
        """Handle the stop event."""
        try:
            self.integration_hub.workload.exec(
                ["kubectl", "delete", "secret", "-l", INTEGRATION_HUB_LABEL, "--all-namespaces"]
            )
        except ExecError:
            self.logger.error(f"Could not delete secret with label {INTEGRATION_HUB_LABEL}")

    @defer_when_not_ready
    def _on_integration_hub_pebble_ready(self, _: PebbleReadyEvent) -> None:
        """Handle on Pebble ready event."""
        self.integration_hub.update()

    @defer_when_not_ready
    def _on_config_changed(self, _: ConfigChangedEvent) -> None:
        """Handle on config changed event."""
        self.integration_hub.update()
