#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Domain object of the Spark Integration Hub charm."""
import json
import logging
from dataclasses import dataclass
from typing import List, MutableMapping

from ops import Application, Relation, Unit

from common.utils import DotSerializer
from relations.spark_sa import IntegrationHubProviderData

logger = logging.getLogger(__name__)


class StateBase:
    """Base state object."""

    def __init__(self, relation: Relation | None, component: Unit | Application):
        self.relation = relation
        self.component = component

    @property
    def relation_data(self) -> MutableMapping[str, str]:
        """The raw relation data."""
        if not self.relation:
            return {}

        return self.relation.data[self.component]

    def update(self, items: dict[str, str]) -> None:
        """Writes to relation_data."""
        if not self.relation:
            return

        self.relation_data.update(items)

    def clear(self) -> None:
        """Clear the content of the relation data."""
        if not self.relation:
            return
        self.relation.data[self.component].clear()


@dataclass
class User:
    """Class representing the user running the Pebble workload services."""

    name: str
    group: str


class S3ConnectionInfo(StateBase):
    """Class representing credentials and endpoints to connect to S3."""

    def __init__(self, relation: Relation, component: Application):
        super().__init__(relation, component)

    @property
    def endpoint(self) -> str | None:
        """Return endpoint of the S3 bucket."""
        return self.relation_data.get("endpoint", None)

    @property
    def access_key(self) -> str:
        """Return the access key."""
        return self.relation_data.get("access-key", "")

    @property
    def secret_key(self) -> str:
        """Return the secret key."""
        return self.relation_data.get("secret-key", "")

    @property
    def path(self) -> str:
        """Return the path in the S3 bucket."""
        return self.relation_data["path"]

    @property
    def bucket(self) -> str:
        """Return the name of the S3 bucket."""
        return self.relation_data["bucket"]

    @property
    def tls_ca_chain(self) -> List[str] | None:
        """Return the CA chain (when applicable)."""
        return (
            json.loads(ca_chain)
            if (ca_chain := self.relation_data.get("tls-ca-chain", ""))
            else None
        )

    @property
    def log_dir(self) -> str:
        """Return the full path to the object."""
        return f"s3a://{self.bucket}/{self.path}"

    @property
    def file_upload_path(self) -> str:
        """Return the path to be used to upload file (eg, by Kyuubi)."""
        return f"s3a://{self.bucket}/"

    @property
    def warehouse_path(self) -> str:
        """Return the path to be used as warehouse."""
        return f"s3a://{self.bucket}/warehouse"


class AzureStorageConnectionInfo:
    """Class representing credentials and endpoints to connect to Azure Storage."""

    def __init__(self, relation_data):
        self.relation_data = relation_data

    @property
    def endpoint(self) -> str | None:
        """Return endpoint of the Azure storage container."""
        if self.connection_protocol in ("abfs", "abfss"):
            return f"{self.connection_protocol}://{self.container}@{self.storage_account}.dfs.core.windows.net"
        elif self.connection_protocol in ("wasb", "wasbs"):
            return f"{self.connection_protocol}://{self.container}@{self.storage_account}.blob.core.windows.net"
        return ""

    @property
    def secret_key(self) -> str:
        """Return the secret key."""
        return self.relation_data["secret-key"]

    @property
    def path(self) -> str:
        """Return the path in the Azure Storage container."""
        return self.relation_data.get("path", "")

    @property
    def container(self) -> str:
        """Return the name of the Azure Storage container."""
        return self.relation_data["container"]

    @property
    def connection_protocol(self) -> str:
        """Return the protocol to be used to access files."""
        return self.relation_data["connection-protocol"].lower()

    @property
    def storage_account(self) -> str:
        """Return the name of the Azure Storage account."""
        return self.relation_data["storage-account"]

    @property
    def log_dir(self) -> str:
        """Return the full path to the object."""
        if self.endpoint:
            return f"{self.endpoint}/{self.path}"
        return ""

    @property
    def file_upload_path(self) -> str:
        """Return the path to be used to upload file (eg, by Kyuubi)."""
        if self.endpoint:
            return f"{self.endpoint}/"
        return ""

    @property
    def warehouse_path(self) -> str:
        """Return the path to be used as warehouse."""
        if self.endpoint:
            return f"{self.endpoint}/warehouse"
        return ""


class PushGatewayInfo(StateBase):
    """Class representing thr endpoints to connect to the prometheus PushGateway."""

    def __init__(self, relation: Relation, component: Application):
        super().__init__(relation, component)

    @property
    def endpoint(self) -> str | None:
        """Return endpoint of the Prometheus PushGateway."""
        raw_data = self.relation_data.get("push-endpoint", None)
        if raw_data:
            data = json.loads(raw_data)
            if "url" in data:
                url = data["url"]
                return url.replace("https://", "").replace("http://", "")
        return None


class HubConfiguration(StateBase):
    """State collection metadata for the peer relation."""

    def __init__(self, relation: Relation | None, component: Application):
        super().__init__(relation, component)
        self.app = component
        self.serializer = DotSerializer()

    def update(self, items):
        """Overridden method to update the hub configuration data."""
        # Workaround for https://bugs.launchpad.net/juju/+bug/2093149
        items = {self.serializer.serialize(k): v for k, v in items.items()}

        return super().update(items)

    @property
    def spark_configurations(self) -> dict[str, str]:
        """Get all Spark configuration options defined by the user."""
        # Workaround for https://bugs.launchpad.net/juju/+bug/2093149
        items = {self.serializer.deserialize(k): v for k, v in self.relation_data.items()}

        return items


class ServiceAccount:
    """Class representing the service account managed by the Spark Integration Hub charm."""

    def __init__(self, relation_data: IntegrationHubProviderData, relation_id: int):
        self.relation_data = relation_data
        self.relation_id = relation_id

    @property
    def service_account(self) -> str | None:
        """Return service account name."""
        return self.relation_data.fetch_my_relation_field(
            relation_id=self.relation_id, field="service-account"
        )

    @property
    def namespace(self) -> str | None:
        """Return the used namespace."""
        return self.relation_data.fetch_my_relation_field(
            relation_id=self.relation_id, field="namespace"
        )

    @property
    def spark_properties(self) -> dict[str, str] | None:
        """Return the set of Spark properties."""
        field_data = (
            self.relation_data.fetch_my_relation_field(
                relation_id=self.relation_id, field="spark-properties"
            )
            or "{}"
        )
        props = dict(json.loads(field_data))
        return dict(sorted(props.items()))

    def set_service_account(self, service_account: str) -> None:
        """Set the service account name."""
        self.relation_data.set_service_account(self.relation_id, service_account)

    def set_namespace(self, namespace: str) -> str | None:
        """Set namespace relation field for this service account."""
        self.relation_data.set_namespace(self.relation_id, namespace)

    def set_spark_properties(self, spark_properties: dict[str, str]) -> None:
        """Set spark-properties relation field for this service account."""
        field_data = json.dumps(spark_properties)
        self.relation_data.set_spark_properties(self.relation_id, field_data)


class LokiURL(StateBase):
    """Class representing the Loki URL managed by the Spark Integration Hub charm."""

    def __init__(self, relation: Relation, component: Unit):
        super().__init__(relation, component)

    @property
    def url(self) -> str | None:
        """Return the Loki URL."""
        endpoint = json.loads(self.relation_data.get("endpoint", "{}"))
        if url := endpoint.get("url"):
            logger.debug("found Loki URL %s in relation data", url)
            return url

        logger.warning("Loki URL was not found in relation data")
        return None
