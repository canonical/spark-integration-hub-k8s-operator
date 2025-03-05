#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Spark Service accounts related event handlers."""

from ops import CharmBase

from common.utils import WithLogging
from constants import INTEGRATION_HUB_REL
from core.context import Context
from core.workload import IntegrationHubWorkloadBase
from events.base import BaseEventHandler, defer_when_not_ready
from managers.integration_hub import IntegrationHubManager
from relations.spark_sa import (
    IntegrationHubProvider,
    ServiceAccountReleasedEvent,
    ServiceAccountRequestedEvent,
)


class IntegrationHubProviderEvents(BaseEventHandler, WithLogging):
    """Class implementing Spark Service Account Integration event hooks."""

    def __init__(self, charm: CharmBase, context: Context, workload: IntegrationHubWorkloadBase):
        super().__init__(charm, "service-account")

        self.charm = charm
        self.context = context
        self.workload = workload

        self.sa = IntegrationHubProvider(self.charm, INTEGRATION_HUB_REL)
        self.integration_hub = IntegrationHubManager(self.workload, self.context)

        self.framework.observe(self.sa.on.account_requested, self._on_service_account_requested)
        self.framework.observe(self.sa.on.account_released, self._on_service_account_released)

    @defer_when_not_ready
    def _on_service_account_requested(self, event: ServiceAccountRequestedEvent):
        """Handle the `ServiceAccountRequested` event for the Spark Integration hub."""
        self.logger.info("Service account requested.")

        if not self.charm.unit.is_leader():
            return
        relation_id = event.relation.id

        service_account = event.service_account
        namespace = event.namespace
        self.logger.debug(
            f"Desired service account name: {service_account} in namespace: {namespace}"
        )

        # Try to create service account
        try:
            self.workload.exec(
                f"python3 -m spark8t.cli.service_account_registry create --username={service_account} --namespace={namespace}"
            )
        except Exception as e:
            self.logger.error(e)
            raise RuntimeError(
                f"Impossible to create service account: {service_account} in namespace: {namespace}"
            )

        self.sa.set_service_account(relation_id, service_account)  # type: ignore
        self.sa.set_namespace(relation_id, namespace)  # type: ignore
        self.integration_hub.update()

    @defer_when_not_ready
    def _on_service_account_released(self, event: ServiceAccountReleasedEvent):
        """Handle the `ServiceAccountReleased` event for the Spark Integration hub."""
        self.logger.info("Service account released.")

        if not self.charm.unit.is_leader():
            return

        service_account = event.service_account
        namespace = event.namespace
        self.logger.debug(
            f"The service account name: {service_account} in namespace: {namespace} should be deleted"
        )

        # Try to create service account
        try:
            self.workload.exec(
                f"python3 -m spark8t.cli.service_account_registry delete --username={service_account} --namespace={namespace}"
            )
        except Exception as e:
            self.logger.error(e)
            raise RuntimeError(
                f"Failed to delete service account: {service_account} in namespace: {namespace}"
            )
