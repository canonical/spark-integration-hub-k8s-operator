#!/usr/bin/env python3
# Copyright 2026 Canonical Limited
# See LICENSE file for licensing details.

"""Service Mesh Integration related event handlers."""

from __future__ import annotations

from typing import TYPE_CHECKING

from charmlibs.interfaces.service_mesh import (
    ServiceMeshConsumer,
)

from common.utils import WithLogging
from core.context import Context
from core.workload import IntegrationHubWorkloadBase
from events.base import BaseEventHandler

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
