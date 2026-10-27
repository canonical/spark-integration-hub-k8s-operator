# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Configuration of live Spark UI exposure."""

from urllib.parse import urlsplit

from charmlibs.interfaces.gateway_metadata import GatewayMetadata
from charms.traefik_k8s.v2.ingress import IngressUrl


class IngressManager:
    """Translate ingress and gateway data into Spark and UI-controller configuration."""

    def __init__(self, owner: str, ingress: IngressUrl | None, gateway: GatewayMetadata | None):
        self.owner = owner
        self.ingress = ingress
        self.gateway = gateway

    @property
    def config(self) -> dict:
        """Return the controller configuration, also when exposure is disabled."""
        enabled = bool(self.ingress and self.gateway)
        return {
            "owner": self.owner,
            "external_url": str(self.ingress.url).rstrip("/")
            if self.ingress and self.gateway
            else None,
            "gateway": self.gateway.model_dump() if enabled and self.gateway else None,
        }

    @property
    def spark_properties(self) -> dict[str, str]:
        """Configure newly submitted applications without a per-application proxyBase."""
        if not (url := self.config["external_url"]):
            return {}
        parsed = urlsplit(url)
        return {
            "spark.ui.proxyRedirectUri": f"{parsed.scheme}://{parsed.netloc}",
        }
