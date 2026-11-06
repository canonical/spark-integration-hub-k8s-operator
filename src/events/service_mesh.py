#!/usr/bin/env python3
# Copyright 2026 Canonical Limited
# See LICENSE file for licensing details.

"""Service Mesh Integration related event handlers."""

from __future__ import annotations

from typing import TYPE_CHECKING

from charmlibs.interfaces.service_mesh import (
    ServiceMeshConsumer,
)
from ops import RelationBrokenEvent

from common.utils import WithLogging
from constants import SERVICE_MESH_RELATION_NAME
from core.context import Context
from core.workload import IntegrationHubWorkloadBase
from events.base import BaseEventHandler, defer_when_not_ready
from managers.integration_hub import IntegrationHubManager

if TYPE_CHECKING:
    from charm import SparkIntegrationHub


class ServiceMeshEvents(BaseEventHandler, WithLogging):
    """Class implementing Ambient Service Mesh event hooks."""

    def __init__(
        self, charm: SparkIntegrationHub, context: Context, workload: IntegrationHubWorkloadBase
    ):
        super().__init__(charm, "service-mesh")

        self.charm = charm
        self.context = context
        self.workload = workload

        self.service_mesh = ServiceMeshConsumer(self.charm)
        self.manager = IntegrationHubManager(workload, context, charm.config)
        self.framework.observe(
            charm.on[SERVICE_MESH_RELATION_NAME].relation_changed, self._on_changed
        )
        self.framework.observe(
            charm.on[SERVICE_MESH_RELATION_NAME].relation_broken, self._on_broken
        )

    @defer_when_not_ready
    def _on_changed(self, _) -> None:
        """Propagate mesh state to Spark and the UI controller."""
        self.manager.update()

    @defer_when_not_ready
    def _on_broken(self, _: RelationBrokenEvent) -> None:
        """Remove stale mesh configuration when the relation breaks."""
        self.manager.update(set_service_mesh_none=True)
