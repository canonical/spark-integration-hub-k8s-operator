#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Charm Context definition and parsing logic."""

from enum import Enum

from charms.data_platform_libs.v0.data_interfaces import DataPeerData
from charms.spark_integration_hub_k8s.v0.spark_service_account import (
    SparkServiceAccountProviderData,
)
from object_storage import AzureStorageRequirer, S3Requirer
from ops import ActiveStatus, BlockedStatus, CharmBase, MaintenanceStatus, Relation

from common.utils import WithLogging
from constants import (
    AZURE_RELATION_NAME,
    INTEGRATION_HUB_REL,
    LOGGING_RELATION_NAME,
    PEER_REL,
    PUSHGATEWAY,
    S3_RELATION_NAME,
)
from core.domain import (
    AzureStorageConnectionInfo,
    HubCluster,
    LokiURL,
    PushGatewayInfo,
    S3ConnectionInfo,
    ServiceAccount,
)


class Context(WithLogging):
    """Properties and relations of the charm."""

    def __init__(self, charm: CharmBase):
        self.charm = charm
        self.model = charm.model

        self.s3_requirer = S3Requirer(self.charm, S3_RELATION_NAME)
        self.azure_storage_requirer = AzureStorageRequirer(
            self.charm,
            AZURE_RELATION_NAME,
        )
        self.spark_service_account_provider_data = SparkServiceAccountProviderData(
            self.model, INTEGRATION_HUB_REL
        )

        self.peer_app_interface = DataPeerData(
            self.model, relation_name=PEER_REL, additional_secret_fields=[]
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
    def _s3_relation(self) -> Relation | None:
        """The S3 relation."""
        return self.charm.model.get_relation(S3_RELATION_NAME)

    @property
    def _azure_storage_relation(self) -> Relation | None:
        """The Azure Storage relation."""
        return self.charm.model.get_relation(AZURE_RELATION_NAME)

    @property
    def _pushgateway_relation(self) -> Relation | None:
        """The Pushgateway relation."""
        return self.charm.model.get_relation(PUSHGATEWAY)

    @property
    def _peer_relation(self) -> Relation | None:
        """The cluster peer relation."""
        return self.model.get_relation(PEER_REL)

    @property
    def cluster(self) -> HubCluster:
        """The cluster state of the current running App."""
        return HubCluster(
            relation=self._peer_relation,
            data_interface=self.peer_app_interface,
            component=self.model.app,
            model_name=self.model.name,
        )

    # --- DOMAIN OBJECTS ---

    @property
    def s3(self) -> S3ConnectionInfo | None:
        """The server state of the current running Unit."""
        relation_data = (
            self.s3_requirer.get_storage_connection_info(self._s3_relation)
            if self._s3_relation
            else None
        )
        return S3ConnectionInfo(relation_data) if relation_data else None

    @property
    def azure_storage(self) -> AzureStorageConnectionInfo | None:
        """The server state of the current running Unit."""
        relation_data = (
            self.azure_storage_requirer.get_storage_connection_info(self._azure_storage_relation)
            if self._azure_storage_relation
            else None
        )
        return AzureStorageConnectionInfo(relation_data) if relation_data else None

    @property
    def pushgateway(self) -> PushGatewayInfo | None:
        """The server state of the current running Unit."""
        return PushGatewayInfo(rel, rel.app) if (rel := self._pushgateway_relation) else None

    @property
    def client_relations(self) -> set[Relation]:
        """The relations of all client applications."""
        return set(self.model.relations[INTEGRATION_HUB_REL])

    @property
    def service_accounts(self) -> list[ServiceAccount]:
        """Retrieve  service account managed by relations.

        Returns:
            List of service accounts/namespaces managed by the Integration Hub
        """
        return [
            ServiceAccount(self.spark_service_account_provider_data, relation.id)
            for relation in self.client_relations
        ]

    @property
    def loki_url(self) -> LokiURL | None:
        """Retrieve Loki URL from logging relations."""
        if relation := self.charm.model.get_relation(LOGGING_RELATION_NAME):
            if units := list(relation.units):
                # select the first unit, because we don't care which unit we get the URL from
                return LokiURL(relation, units[0])

        return None


class Status(Enum):
    """Class bundling all statuses that the charm may fall into."""

    WAITING_PEBBLE = MaintenanceStatus("Waiting for Pebble")
    INVALID_S3_CREDENTIALS = BlockedStatus("Invalid S3 credentials")
    NOT_RUNNING = BlockedStatus("Integration Hub is not running. Please check logs.")
    NOT_TRUSTED = BlockedStatus("Run `juju trust {app} --scope=cluster`. Needed to run.")
    MULTIPLE_OBJECT_STORAGE_RELATIONS = BlockedStatus(
        "Integration Hub can be related to only one storage backend at a time."
    )
    MULTIPLE_UNITS = BlockedStatus(
        "Integration Hub can be run with only one unit. Please scale down to 1 unit."
    )
    ACTIVE = ActiveStatus("")
