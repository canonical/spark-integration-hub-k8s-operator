#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Charmed Kubernetes Operator for the Spark Integration Hub Charm."""

from charms.data_platform_libs.v0.data_models import TypedCharmBase
from ops import CollectStatusEvent, main

from common.utils import WithLogging
from constants import CONTAINER, PEBBLE_USER
from core.config import CharmConfig
from core.context import Context, Status
from core.domain import User
from events.azure_storage import AzureStorageEvents
from events.integration_hub import IntegrationHubEvents
from events.logging import LoggingEvents
from events.provider import SparkServiceAccountProviderEvents
from events.pushgateway import PushgatewayEvents
from events.s3 import S3Events
from managers.k8s import KubernetesManager
from managers.s3 import S3Manager
from workload import IntegrationHub


class SparkIntegrationHub(TypedCharmBase[CharmConfig], WithLogging):
    """Charm the service."""

    config_type = CharmConfig

    def __init__(self, *args):
        super().__init__(*args)

        self.context = Context(self)
        self.workload = IntegrationHub(
            self.unit.get_container(CONTAINER), User(name=PEBBLE_USER[0], group=PEBBLE_USER[1])
        )

        self.s3 = S3Events(self, self.context, self.workload)
        self.azure_storage = AzureStorageEvents(self, self.context, self.workload)
        self.sa = SparkServiceAccountProviderEvents(self, self.context, self.workload)
        self.pushgateway = PushgatewayEvents(self, self.context, self.workload)
        self.integration_hub = IntegrationHubEvents(self, self.context, self.workload)
        self.logging = LoggingEvents(self, self.context, self.workload)

        self.framework.observe(self.on.collect_unit_status, self._on_collect_status)
        self.framework.observe(self.on.collect_app_status, self._on_collect_status)

    def _on_collect_status(self, event: CollectStatusEvent) -> None:
        """Set the status of the app/unit.

        This must be the only place in the codebase where we set the app/unit status.
        """
        if not self.workload.ready():
            event.add_status(Status.WAITING_PEBBLE.value)
            return

        k8s_manager = KubernetesManager(self.model.app.name)
        if not k8s_manager.trusted():
            status = Status.NOT_TRUSTED.value
            status.message = status.message.format(app=str(self.model.app.name))
            event.add_status(status)

        if self.context.s3 and self.context.azure_storage:
            event.add_status(Status.MULTIPLE_OBJECT_STORAGE_RELATIONS.value)

        if self.context.s3:
            s3_manager = S3Manager(self.context.s3)
            if not s3_manager.verify():
                event.add_status(Status.INVALID_S3_CREDENTIALS.value)

        if not self.workload.active():
            event.add_status(Status.NOT_RUNNING.value)

        event.add_status(Status.ACTIVE.value)


if __name__ == "__main__":  # pragma: nocover
    main(SparkIntegrationHub)
