#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""Event handlers for configuration-related Juju Actions."""
import logging
import re
from typing import TYPE_CHECKING, Tuple

from ops.charm import ActionEvent
from ops.framework import Object

from core.context import Context
from core.workload import IntegrationHubWorkloadBase

if TYPE_CHECKING:
    from charm import SparkIntegrationHub

logger = logging.getLogger(__name__)


class ConfigurationActionEvents(Object):
    """Event handlers for configuration-related Juju Actions."""

    def __init__(self, charm, context: Context, workload: IntegrationHubWorkloadBase):
        super().__init__(charm, "configutation_events")
        self.charm: "SparkIntegrationHub" = charm
        self.context = context
        self.workload = workload

        self.framework.observe(
            getattr(self.charm.on, "add_config_action"), self._add_config_action
        )

        self.framework.observe(
            getattr(self.charm.on, "remove_config_action"), self._remove_config_action
        )

        self.framework.observe(
            getattr(self.charm.on, "clear_config_action"), self._clear_config_action
        )

        self.framework.observe(
            getattr(self.charm.on, "list_config_action"), self._list_config_action
        )

    def _add_config_action(self, event: ActionEvent) -> None:
        """Add configuration action."""
        if not self.workload.ready():
            msg = "Charm is not ready"
            logger.error(msg)
            event.fail(msg)
            return

        if not self.context.peer_relation:
            msg = "Peer relation is not ready"
            logger.error(msg)
            event.fail(msg)
            return

        config = event.params["conf"]

        # parse config option
        if "=" in config:
            key, value = self._parse_property_line(config)
            logger.debug(self.context.hub_configurations.relation_data)
            self.context.hub_configurations.relation_data.update({key: value})
            event.set_results({"added-config": f"{key}:{value}"})
            return

        msg = f"Configuration {config} cannot be applied"
        logger.error(msg)
        event.fail(msg)
        return

    def _remove_config_action(self, event: ActionEvent) -> None:
        """Remove configuration action."""
        if not self.workload.ready():
            msg = "Charm is not ready"
            logger.error(msg)
            event.fail(msg)
            return
        if not self.context.peer_relation:
            msg = "Peer relation is not ready"
            logger.error(msg)
            event.fail(msg)
            return

        key = event.params["key"]
        if key in self.context.hub_configurations.relation_data:
            self.context.hub_configurations.relation_data.update({key: ""})  # type: ignore
            event.set_results({"removed-key": key})
            return

        msg = f"Configuration {key} is not present"
        logger.error(msg)
        event.fail(msg)
        return

    def _clear_config_action(self, event: ActionEvent) -> None:
        """Clear configuration action."""
        if not self.workload.ready():
            msg = "Charm is not ready"
            logger.error(msg)
            event.fail(msg)
            return

        if not self.context.peer_relation:
            msg = "Peer relation is not ready"
            logger.error(msg)
            event.fail(msg)
            return

        self.context.hub_configurations.relation_data.clear()
        event.set_results({"current-configuration": ""})
        return

    def _list_config_action(self, event: ActionEvent) -> None:
        """List configuration action."""
        if not self.workload.ready():
            msg = "Charm is not ready"
            logger.error(msg)
            event.fail(msg)
            return

        if not self.context.peer_relation:
            msg = "Peer relation is not ready"
            logger.error(msg)
            event.fail(msg)
            return

        configuration = self.context.hub_configurations.spark_configurations
        logger.info(f"Get configuration: {configuration}")
        event.set_results(configuration)
        return

    def _parse_property_line(self, line: str) -> Tuple[str, str]:  # type: ignore
        prop_assignment = list(filter(None, re.split("=| ", line.strip())))
        prop_key = prop_assignment[0].strip()
        option_assignment = line.split("=", 1)
        value = option_assignment[1].strip()
        return prop_key, value
