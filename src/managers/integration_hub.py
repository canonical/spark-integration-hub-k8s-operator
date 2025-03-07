#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Integration Hub manager."""

import re

from common.utils import WithLogging
from core.context import Context
from core.domain import (
    AzureStorageConnectionInfo,
    HubConfiguration,
    LokiURL,
    PushGatewayInfo,
    S3ConnectionInfo,
)
from core.workload import IntegrationHubWorkloadBase
from managers.azure_storage import AzureStorageManager
from managers.s3 import S3Manager


class IntegrationHubConfig(WithLogging):
    """Class representing the Spark Properties configuration file."""

    _ingress_pattern = re.compile("http://.*?/|https://.*?/")

    _base_conf: dict[str, str] = {}

    def __init__(
        self,
        s3: S3ConnectionInfo | None,
        azure_storage: AzureStorageConnectionInfo | None,
        pushgateway: PushGatewayInfo | None,
        hub_conf: HubConfiguration | None,
        loki_url: LokiURL | None,
    ):
        self.s3 = S3Manager(s3) if s3 else None
        self.azure_storage = AzureStorageManager(azure_storage) if azure_storage else None
        self.pushgateway = pushgateway
        self.hub_conf = hub_conf
        self.loki_url = loki_url

    @staticmethod
    def _ssl_enabled(endpoint: str | None) -> str:
        """Check if ssl is enabled."""
        if not endpoint or endpoint.startswith("https:") or ":443" in endpoint:
            return "true"

        return "false"

    @property
    def _log_forwarding_conf(self) -> dict[str, str]:
        """Get log forwarding configuration."""
        if not self.loki_url:
            self.logger.debug("Log forwarding is disabled.")
            return {}

        self.logger.debug("Log forwarding is enabled to %s.", self.loki_url.url)
        return {
            "spark.executorEnv.LOKI_URL": self.loki_url.url or "",
            "spark.kubernetes.driverEnv.LOKI_URL": self.loki_url.url or "",
        }

    @property
    def _s3_conf(self) -> dict[str, str]:
        if (s3 := self.s3) and s3.verify():
            return {
                "spark.hadoop.fs.s3a.path.style.access": "true",
                "spark.eventLog.enabled": "true",
                "spark.hadoop.fs.s3a.endpoint": s3.config.endpoint or "https://s3.amazonaws.com",
                "spark.hadoop.fs.s3a.access.key": s3.config.access_key,
                "spark.hadoop.fs.s3a.secret.key": s3.config.secret_key,
                "spark.eventLog.dir": s3.config.log_dir,
                "spark.history.fs.logDirectory": s3.config.log_dir,
                "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
                "spark.hadoop.fs.s3a.connection.ssl.enabled": self._ssl_enabled(
                    s3.config.endpoint
                ),
                "spark.kubernetes.file.upload.path": s3.config.file_upload_path,
                "spark.sql.warehouse.dir": s3.config.warehouse_path,
            }
        return {}

    @property
    def _azure_storage_conf(self) -> dict[str, str]:
        if azure_storage := self.azure_storage:
            confs = {
                "spark.eventLog.enabled": "true",
                "spark.eventLog.dir": azure_storage.config.log_dir,
                "spark.history.fs.logDirectory": azure_storage.config.log_dir,
                "spark.kubernetes.file.upload.path": azure_storage.config.file_upload_path,
                "spark.sql.warehouse.dir": azure_storage.config.warehouse_path,
            }
            connection_protocol = azure_storage.config.connection_protocol
            if connection_protocol.lower() in ("abfss", "abfs"):
                confs.update(
                    {
                        f"spark.hadoop.fs.azure.account.key.{azure_storage.config.storage_account}.dfs.core.windows.net": azure_storage.config.secret_key
                    }
                )
            elif connection_protocol.lower() in ("wasb", "wasbs"):
                confs.update(
                    {
                        f"spark.hadoop.fs.azure.account.key.{azure_storage.config.storage_account}.blob.core.windows.net": azure_storage.config.secret_key
                    }
                )
            return confs
        return {}

    @property
    def _pushgateway_conf(self) -> dict[str, str]:
        if pg := self.pushgateway:
            return {
                "spark.metrics.conf.*.sink.prometheus.pushgateway-address": pg.endpoint,  # type: ignore
                "spark.metrics.conf.*.sink.prometheus.class": "org.apache.spark.banzaicloud.metrics.sink.PrometheusSink",
                "spark.metrics.conf.*.sink.prometheus.enable-dropwizard-collector": "true",
                "spark.metrics.conf.*.sink.prometheus.period": "5",
                "spark.metrics.conf.*.sink.prometheus.metrics-name-capture-regex": "([a-z0-9]*_[a-z0-9]*_[a-z0-9]*_)(.+)",
                "spark.metrics.conf.*.sink.prometheus.metrics-name-replacement": "$2",
            }
        return {}

    @property
    def _action_conf(self) -> dict[str, str]:
        if a_conf := self.hub_conf:
            return a_conf.spark_configurations
        return {}

    def to_dict(self) -> dict[str, str]:
        """Return the dict representation of the configuration file."""
        to_return = (
            self._base_conf
            | self._s3_conf
            | self._azure_storage_conf
            | self._pushgateway_conf
            | self._action_conf
            | self._log_forwarding_conf
        )
        return to_return

    @property
    def contents(self) -> str:
        """Return configuration contents formatted to be consumed by pebble layer."""
        dict_content = self.to_dict()

        return "\n".join(
            [
                f"{key}={value}"
                for key in sorted(dict_content.keys())
                if (value := dict_content[key])
            ]
        )


class IntegrationHubManager(WithLogging):
    """Class exposing general functionalities of the IntegrationHub workload."""

    def __init__(self, workload: IntegrationHubWorkloadBase, context: Context):
        self.workload = workload
        self.context = context

    def _compare_and_update_file(self, content: str, file_path: str) -> bool:
        """Update the file at given file_path with given content.

        Before doing the update, compare the existing content of the file and update
        it only if has changed.

        Return True if the file was re-written, else False.
        """
        try:
            existing_content = self.workload.read(file_path)
            file_exists = True
        except FileNotFoundError:
            existing_content = ""
            file_exists = False
        self.logger.debug(f"{file_path=}")
        self.logger.debug(f"{existing_content=}")
        self.logger.debug(f"{content=}")
        if not file_exists or existing_content != content:
            self.workload.write(content, file_path)
            return True

        return False

    def update(
        self,
        set_s3_none: bool = False,
        set_azure_storage_none: bool = False,
        set_pushgateway_none: bool = False,
        set_loki_url_none: bool = False,
    ) -> None:
        """Update the Integration Hub service if needed."""
        s3 = None if set_s3_none else self.context.s3
        azure_storage = None if set_azure_storage_none else self.context.azure_storage
        pushgateway = None if set_pushgateway_none else self.context.pushgateway
        loki_url = None if set_loki_url_none else self.context.loki_url

        self.logger.debug("Update")
        self.workload.stop()

        config = IntegrationHubConfig(s3, azure_storage, pushgateway, hub_conf, loki_url)

        if self._compare_and_update_file(
            config.contents, str(self.workload.paths.spark_properties)
        ):
            self.logger.info("Updating integration hub config...")
            self.workload.set_environment(
                {"SPARK_PROPERTIES_FILE": str(self.workload.paths.spark_properties)}
            )
            self.workload.restart()

        if self.context.charm.unit.is_leader():
            for sa in self.context.service_accounts:
                if sa.spark_properties != config.to_dict():
                    sa.set_spark_properties(config.to_dict())
