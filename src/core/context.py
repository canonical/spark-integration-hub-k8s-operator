#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Charm Context definition and parsing logic."""

from enum import Enum

from charms.data_platform_libs.v0.data_interfaces import RequirerData
from ops import ActiveStatus, BlockedStatus, CharmBase, MaintenanceStatus, Relation

from common.utils import WithLogging
from constants import PUSHGATEWAY, S3
from core.domain import PushGatewayInfo, S3ConnectionInfo


class Context(WithLogging):
    """Properties and relations of the charm."""

    def __init__(self, charm: CharmBase):

        self.charm = charm
        self.model = charm.model

        self.s3_endpoint = RequirerData(self.charm.model, S3)

    # --------------
    # --- CONFIG ---
    # --------------
    # We don't have config yet in the Spark Integration hub charm.
    # --------------

    # -----------------
    # --- RELATIONS ---
    # -----------------

    @property
    def _s3_relation_id(self) -> int | None:
        """The S3 relation."""
        return relation.id if (relation := self.charm.model.get_relation(S3)) else None

    @property
    def _s3_relation(self) -> Relation | None:
        """The S3 relation."""
        return self.charm.model.get_relation(S3)

    @property
    def _pushgateway_relation_id(self) -> int | None:
        """The Pushgateway relation."""
        return relation.id if (relation := self.charm.model.get_relation(PUSHGATEWAY)) else None

    @property
    def _pushgateway_relation(self) -> Relation | None:
        """The Pushgateway relation."""
        return self.charm.model.get_relation(PUSHGATEWAY)

    # --- DOMAIN OBJECTS ---

    @property
    def s3(self) -> S3ConnectionInfo | None:
        """The server state of the current running Unit."""
        return S3ConnectionInfo(rel, rel.app) if (rel := self._s3_relation) else None

    @property
    def pushgateway(self) -> PushGatewayInfo | None:
        """The server state of the current running Unit."""
        return PushGatewayInfo(rel, rel.app) if (rel := self._pushgateway_relation) else None


class Status(Enum):
    """Class bundling all statuses that the charm may fall into."""

    WAITING_PEBBLE = MaintenanceStatus("Waiting for Pebble")
    INVALID_CREDENTIALS = BlockedStatus("Invalid S3 credentials")
    NOT_RUNNING = BlockedStatus("Integration Hub is not running. Please check logs.")
    NOT_TRUSTED = BlockedStatus("Integration Hub is not trusted! Please check logs.")
    ACTIVE = ActiveStatus("")
