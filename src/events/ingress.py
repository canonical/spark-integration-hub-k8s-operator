# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Relation events for live Spark UI ingress."""

from typing import TYPE_CHECKING

from charms.traefik_k8s.v2.ingress import IngressPerAppRequirer
from ops import RelationBrokenEvent

from constants import GATEWAY_METADATA_RELATION_NAME, INGRESS_RELATION_NAME, SPARK_UI_PORT
from core.context import Context
from core.workload import IntegrationHubWorkloadBase
from events.base import BaseEventHandler, defer_when_not_ready
from managers.integration_hub import IntegrationHubManager

if TYPE_CHECKING:
    from charm import SparkIntegrationHub


class IngressEvents(BaseEventHandler):
    """Reconcile the workload when either ingress relation changes."""

    def __init__(
        self, charm: "SparkIntegrationHub", context: Context, workload: IntegrationHubWorkloadBase
    ):
        super().__init__(charm, "ingress")
        self.charm = charm
        self.context = context
        self.workload = workload
        self.manager = IntegrationHubManager(workload, context, charm.config)
        self.ingress = IngressPerAppRequirer(
            charm, relation_name=INGRESS_RELATION_NAME, port=SPARK_UI_PORT, strip_prefix=True
        )
        self.framework.observe(self.ingress.on.ready, self._on_changed)
        self.framework.observe(self.ingress.on.revoked, self._on_changed)
        self.framework.observe(
            charm.on[GATEWAY_METADATA_RELATION_NAME].relation_changed, self._on_changed
        )
        self.framework.observe(
            charm.on[INGRESS_RELATION_NAME].relation_broken, self._on_ingress_broken
        )
        self.framework.observe(
            charm.on[GATEWAY_METADATA_RELATION_NAME].relation_broken, self._on_gateway_broken
        )

    @defer_when_not_ready
    def _on_changed(self, _) -> None:
        """Update Spark properties and UI controller settings."""
        self.manager.update()

    @defer_when_not_ready
    def _on_ingress_broken(self, _: RelationBrokenEvent) -> None:
        """Disable exposure when the ingress relation is removed."""
        self.manager.update(set_ingress_none=True)

    @defer_when_not_ready
    def _on_gateway_broken(self, _: RelationBrokenEvent) -> None:
        """Disable exposure when the gateway-metadata relation is removed."""
        self.manager.update(set_gateway_metadata_none=True)
