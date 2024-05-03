#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""Application charm that connects to the Spark Integration hub charm.

This charm is meant to be used only for testing
of the libraries in this repository.
"""

import logging

from ops.charm import CharmBase
from ops.main import main
from ops.model import ActiveStatus

from relations.spark_sa import IntegrationHubRequirer, ServiceAccountGrantedEvent

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

        self.sa1 = IntegrationHubRequirer(
            self, relation_name=REL_NAME_A, service_account="sa1", namespace=namespace
        )
        self.sa2 = IntegrationHubRequirer(
            self, relation_name=REL_NAME_B, service_account="sa2", namespace=namespace
        )

        self.framework.observe(self.sa1.on.account_granted, self.on_account_granted_sa_1)
        self.framework.observe(self.sa2.on.account_granted, self.on_account_granted_sa_2)

    def _on_start(self, _) -> None:
        self.unit.status = ActiveStatus()

    def on_account_granted_sa_1(self, event: ServiceAccountGrantedEvent) -> None:
        logger.info(f"{event.service_account} {event.namespace}")
        return

    def on_account_granted_sa_2(self, event: ServiceAccountGrantedEvent) -> None:
        logger.info(f"{event.service_account} {event.namespace}")
        return


if __name__ == "__main__":
    main(ApplicationCharm)
