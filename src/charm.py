#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Charmed Kubernetes Operator for the Spark Integration Hub Charm."""

from ops import CharmBase
from ops.main import main

from common.utils import WithLogging
from constants import CONTAINER, PEBBLE_USER
from core.context import Context
from core.domain import User
from events.configuration_actions import ConfigurationActionEvents
from events.integration_hub import IntegrationHubEvents
from events.provider import IntegrationHubProviderEvents
from events.pushgateway import PushgatewayEvents
from events.s3 import S3Events
from workload import IntegrationHub


class SparkIntegrationHub(CharmBase, WithLogging):
    """Charm the service."""

    def __init__(self, *args):
        super().__init__(*args)

        context = Context(self)
        workload = IntegrationHub(
            self.unit.get_container(CONTAINER), User(name=PEBBLE_USER[0], group=PEBBLE_USER[1])
        )

        self.s3 = S3Events(self, context, workload)
        self.configuration_action_events = ConfigurationActionEvents(self, context, workload)
        self.sa = IntegrationHubProviderEvents(self, context, workload)
        self.pushgateway = PushgatewayEvents(self, context, workload)
        self.integration_hub = IntegrationHubEvents(self, context, workload)


if __name__ == "__main__":  # pragma: nocover
    main(SparkIntegrationHub)
