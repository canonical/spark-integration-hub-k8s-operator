#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Charmed Kubernetes Operator for the Spark Integration Hub Charm."""

import ops

from common.utils import WithLogging
from constants import CONTAINER, PEBBLE_USER
from core.context import Context
from core.domain import User
from events.azure_storage import AzureStorageEvents
from events.integration_hub import IntegrationHubEvents
from events.logging import LoggingEvents
from events.provider import SparkServiceAccountProviderEvents
from events.pushgateway import PushgatewayEvents
from events.s3 import S3Events
from workload import IntegrationHub


class SparkIntegrationHub(ops.CharmBase, WithLogging):
    """Charm the service."""

    def __init__(self, *args):
        super().__init__(*args)

        context = Context(self)
        workload = IntegrationHub(
            self.unit.get_container(CONTAINER), User(name=PEBBLE_USER[0], group=PEBBLE_USER[1])
        )

        self.s3 = S3Events(self, context, workload)
        self.azure_storage = AzureStorageEvents(self, context, workload)
        self.sa = SparkServiceAccountProviderEvents(self, context, workload)
        self.pushgateway = PushgatewayEvents(self, context, workload)
        self.integration_hub = IntegrationHubEvents(self, context, workload)
        self.logging = LoggingEvents(self, context, workload)


if __name__ == "__main__":  # pragma: nocover
    ops.main(SparkIntegrationHub)
