#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Charm Context definition and parsing logic."""

from enum import Enum
from typing import List

from charms.data_platform_libs.v0.data_interfaces import RequirerData
from ops import ActiveStatus, BlockedStatus, CharmBase, MaintenanceStatus, Relation

from common.utils import WithLogging
from constants import AZURE_RELATION_NAME, INTEGRATION_HUB_REL, PEER, PUSHGATEWAY, S3_RELATION_NAME
from core.domain import (
    AzureStorageConnectionInfo,
    HubConfiguration,
    PushGatewayInfo,
    S3ConnectionInfo,
    ServiceAccount,
)


class Context(WithLogging):
    """Properties and relations of the charm."""

    def __init__(self, charm: CharmBase):

        self.charm = charm
        self.model = charm.model

        self.s3_endpoint = RequirerData(self.charm.model, S3_RELATION_NAME)
        self.azure_storage_endpoint = RequirerData(
            self.charm.model, AZURE_RELATION_NAME, additional_secret_fields=["secret-key"]
        )

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
        return (
            relation.id if (relation := self.charm.model.get_relation(S3_RELATION_NAME)) else None
        )

    @property
    def _s3_relation(self) -> Relation | None:
        """The S3 relation."""
        return self.charm.model.get_relation(S3_RELATION_NAME)

    @property
    def _azure_storage_relation_id(self) -> int | None:
        """The Azure Storage relation ID."""
        return (
            relation.id
            if (relation := self.charm.model.get_relation(AZURE_RELATION_NAME))
            else None
        )

    @property
    def _azure_storage_relation(self) -> Relation | None:
        """The Azure Storage relation."""
        return self.charm.model.get_relation(AZURE_RELATION_NAME)

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
    def azure_storage(self) -> AzureStorageConnectionInfo | None:
        """The server state of the current running Unit."""
        relation_data = (
            self.azure_storage_endpoint.fetch_relation_data()[self._azure_storage_relation_id]
            if self._azure_storage_relation
            else None
        )
        return AzureStorageConnectionInfo(relation_data) if relation_data else None

    @property
    def pushgateway(self) -> PushGatewayInfo | None:
        """The server state of the current running Unit."""
        return PushGatewayInfo(rel, rel.app) if (rel := self._pushgateway_relation) else None

    @property
    def peer_relation(self) -> Relation | None:
        """The hub spark configuration peer relation."""
        return self.model.get_relation(PEER)

    @property
    def hub_configurations(self) -> HubConfiguration | None:
        """The spark configuration of the current running Hub."""
        return HubConfiguration(relation=self.peer_relation, component=self.model.app)

    @property
    def client_relations(self) -> set[Relation]:
        """The relations of all client applications."""
        return set(self.model.relations[INTEGRATION_HUB_REL])

    @property
    def services_accounts(self) -> List[ServiceAccount]:
        """Retrieve  service account managed by relations.

        Returns:
            List of service accounts/namespaces managed by the Integration Hub
        """
        return [
            ServiceAccount(relation, relation.app)
            for relation in self.client_relations
            if not relation or not relation.app
        ]


class Status(Enum):
    """Class bundling all statuses that the charm may fall into."""

    WAITING_PEBBLE = MaintenanceStatus("Waiting for Pebble")
    INVALID_S3_CREDENTIALS = BlockedStatus("Invalid S3 credentials")
    NOT_RUNNING = BlockedStatus("Integration Hub is not running. Please check logs.")
    NOT_TRUSTED = BlockedStatus("Integration Hub is not trusted! Please check logs.")
    MULTIPLE_OBJECT_STORAGE_RELATIONS = BlockedStatus(
        "Integration Hub can be related to only one storage backend at a time."
    )
    ACTIVE = ActiveStatus("")
