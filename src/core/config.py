# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.


"""Structured configuration for the Integration Hub charm."""

import logging
import re

from charms.data_platform_libs.v0.data_models import BaseConfigModel
from pydantic import Field, validator

logger = logging.getLogger(__name__)


class CharmConfig(BaseConfigModel):
    """Manager for the structured configuration."""

    enable_dynamic_allocation: bool
    driver_pod_template: str
    executor_pod_template: str
    monitored_service_accounts: list[str] = Field(default="")
    spark_image: str

    @validator("monitored_service_accounts", pre=True)
    @classmethod
    def monitored_service_accounts_validator(cls, value: str) -> list[str]:
        """Check validity of `monitored-service-accounts` field."""
        if not value:
            return []
        validated = []
        for sa in value.split(","):
            if sa.count(":") == 1:
                key, val = sa.split(":", 1)
                pattern = r"[a-z0-9A-Z\*](?:[a-z0-9\-\*]{0,61}[a-z0-9\*])?$"
                if not (re.match(pattern, key) and re.match(pattern, val)):
                    raise ValueError(f"Malformed service accounts: {key}:{val}")
                validated.append(sa)

            else:
                raise ValueError("Malformed monitored-service-accounts options.")
        return validated
