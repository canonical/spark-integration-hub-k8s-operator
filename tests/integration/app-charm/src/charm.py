#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""Application charm that connects to the Spark Integration hub charm.

This charm is meant to be used only for testing
of the libraries in this repository.
"""

import logging

from ops.charm import ActionEvent, CharmBase
from ops.main import main
from ops.model import ActiveStatus

from relations.spark_sa import ServiceAccountGrantedEvent, SparkServiceAccountRequirer

logger = logging.getLogger(__name__)


CHARM_KEY = "app"
REL_NAME_A = "spark-account-a"
REL_NAME_B = "spark-account-b"


class ApplicationCharm(CharmBase):
    """Application charm that connects to the Spark Integration Hub charm."""

    def __init__(self, *args):
        super().__init__(*args)
        self.name = CHARM_KEY

        self.framework.observe(getattr(self.on, "start"), self._on_start)

        namespace = self.config["namespace"]

        self.sa1 = SparkServiceAccountRequirer(
            self, relation_name=REL_NAME_A, service_account=f"{namespace}:sa1"
        )
        self.sa2 = SparkServiceAccountRequirer(
            self, relation_name=REL_NAME_B, service_account=f"{namespace}:sa2"
        )

        self.framework.observe(self.sa1.on.account_granted, self.on_account_granted_sa_1)
        self.framework.observe(self.sa2.on.account_granted, self.on_account_granted_sa_2)

        self.framework.observe(self.on.get_properties_sa1_action, self._get_properties_sa1_action)
        self.framework.observe(self.on.get_properties_sa2_action, self._get_properties_sa2_action)

    def _on_start(self, _) -> None:
        self.unit.status = ActiveStatus()

    def on_account_granted_sa_1(self, event: ServiceAccountGrantedEvent) -> None:
        logger.info(f"{event.service_account}")
        return

    def on_account_granted_sa_2(self, event: ServiceAccountGrantedEvent) -> None:
        logger.info(f"{event.service_account}")
        return

    def _get_properties_sa1_action(self, event: ActionEvent) -> None:
        relation = self.model.get_relation(relation_name=REL_NAME_A)
        if relation is None:
            return
        properties = self.sa1.fetch_relation_field(relation.id, "spark-properties")
        event.set_results({"spark-properties": properties})
        return

    def _get_properties_sa2_action(self, event: ActionEvent) -> None:
        relation = self.model.get_relation(relation_name=REL_NAME_B)
        if relation is None:
            return
        properties = self.sa2.fetch_relation_field(relation.id, "spark-properties")
        event.set_results({"spark-properties": properties})
        return


if __name__ == "__main__":
    main(ApplicationCharm)
