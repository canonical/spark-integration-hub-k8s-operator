#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Integration Hub manager."""

import os
import re
from pathlib import Path
from urllib.parse import ParseResult, urlparse

from common.utils import (
    WithLogging,
    get_hub_secret_manifest,
    get_hub_truststore_secret_manifest,
    is_proxy_skipped,
)
from core.config import CharmConfig
from core.context import Context
from core.domain import (
    AzureStorageConnectionInfo,
    LokiURL,
    PushGatewayInfo,
    S3ConnectionInfo,
)
from core.workload import IntegrationHubWorkloadBase
from managers.azure_storage import AzureStorageManager
from managers.s3 import S3Manager
from managers.tls import TLSManager


class IntegrationHubConfig(WithLogging):
    """Class representing the Spark Properties configuration file."""

    _ingress_pattern = re.compile("http://.*?/|https://.*?/")

    _base_conf: dict[str, str] = {}

    def __init__(
        self,
        context: Context,
        s3: S3ConnectionInfo | None,
        azure_storage: AzureStorageConnectionInfo | None,
        pushgateway: PushGatewayInfo | None,
        hub_conf: CharmConfig,
        loki_url: LokiURL | None,
    ):
        self.context = context
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
        if (s3 := self.s3) is None or not s3.verify():
            return {}

        base_s3_conf = {
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.eventLog.enabled": "true",
            "spark.hadoop.fs.s3a.endpoint": s3.connection_info.endpoint
            or "https://s3.amazonaws.com",
            "spark.hadoop.fs.s3a.access.key": s3.connection_info.access_key,
            "spark.hadoop.fs.s3a.secret.key": s3.connection_info.secret_key,
            "spark.eventLog.dir": s3.connection_info.log_dir,
            "spark.history.fs.logDirectory": s3.connection_info.log_dir,
            "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": self._ssl_enabled(
                s3.connection_info.endpoint
            ),
            "spark.kubernetes.file.upload.path": s3.connection_info.file_upload_path,
            "spark.sql.warehouse.dir": s3.connection_info.warehouse_path,
        }

        if s3.connection_info.tls_ca_chain:
            truststore_password = self.context.cluster.truststore_password
            truststore_filename = os.path.basename(Path(self.context.cluster.truststore_path))
            truststore_secret_name = self.context.cluster.truststore_secret_name

            base_s3_conf["spark.driver.extraJavaOptions"] = (
                f"-Djavax.net.ssl.trustStore=/{truststore_secret_name}/{truststore_filename} -Djavax.net.ssl.trustStorePassword={truststore_password}"
            )
            base_s3_conf["spark.executor.extraJavaOptions"] = (
                f"-Djavax.net.ssl.trustStore=/{truststore_secret_name}/{truststore_filename} -Djavax.net.ssl.trustStorePassword={truststore_password}"
            )
            base_s3_conf[f"spark.kubernetes.executor.secrets.{truststore_secret_name}"] = (
                truststore_secret_name
            )
            base_s3_conf[f"spark.kubernetes.driver.secrets.{truststore_secret_name}"] = (
                truststore_secret_name
            )
            base_s3_conf["spark.hadoop.fs.s3a.connection.ssl.enabled"] = "true"

        s3_scheme = urlparse(s3.connection_info.endpoint).scheme
        proxy_url = {
            "http": os.environ.get("JUJU_CHARM_HTTP_PROXY", ""),
            "https": os.environ.get("JUJU_CHARM_HTTPS_PROXY", ""),
        }.get(s3_scheme, os.environ.get("JUJU_CHARM_HTTP_PROXY", ""))

        if is_proxy_skipped(s3.connection_info.endpoint):
            proxy_conf: dict[str, str] = {}
        else:
            match urlparse(proxy_url):
                case ParseResult(
                    username=str(username),
                    password=str(password),
                    hostname=str(hostname),
                    port=port,
                    scheme=scheme,
                ) if scheme in ("http", "https"):
                    port_str = str(port) if port else {"http": "80", "https": "443"}[scheme]
                    proxy_conf = {
                        "spark.hadoop.fs.s3a.proxy.host": hostname,
                        "spark.hadoop.fs.s3a.proxy.ssl.enabled": "true"
                        if scheme == "https"
                        else "false",
                        "spark.hadoop.fs.s3a.proxy.port": port_str,
                        "spark.hadoop.fs.s3a.proxy.username": username,
                        "spark.hadoop.fs.s3a.proxy.password": password,
                    }

                case ParseResult(
                    username=None,
                    password=None,
                    hostname=str(hostname),
                    port=port,
                    scheme=scheme,
                ) if scheme in ("http", "https"):
                    port_str = str(port) if port else {"http": "80", "https": "443"}[scheme]
                    proxy_conf = {
                        "spark.hadoop.fs.s3a.proxy.host": hostname,
                        "spark.hadoop.fs.s3a.proxy.ssl.enabled": "true"
                        if scheme == "https"
                        else "false",
                        "spark.hadoop.fs.s3a.proxy.port": port_str,
                    }

                case _:
                    proxy_conf = {}

        return base_s3_conf | proxy_conf

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
    def _hub_conf(self) -> dict[str, str]:
        hub_conf: dict[str, str] = {}
        if self.hub_conf.enable_dynamic_allocation:
            hub_conf.update(
                {
                    "spark.dynamicAllocation.enabled": "true",
                    "spark.dynamicAllocation.shuffleTracking.enabled": "true",
                    "spark.dynamicAllocation.minExecutors": "1",
                }
            )
        if dpt := self.hub_conf.driver_pod_template:
            hub_conf.update(
                {
                    "spark.kubernetes.driver.podTemplateFile": dpt,
                }
            )
        if ept := self.hub_conf.executor_pod_template:
            hub_conf.update(
                {
                    "spark.kubernetes.executor.podTemplateFile": ept,
                }
            )
        if spark_image := self.hub_conf.spark_image:
            hub_conf.update(
                {
                    "spark.kubernetes.container.image": spark_image,
                }
            )

        return hub_conf

    def to_dict(self) -> dict[str, str]:
        """Return the dict representation of the configuration file."""
        to_return = (
            self._base_conf
            | self._s3_conf
            | self._azure_storage_conf
            | self._pushgateway_conf
            | self._hub_conf
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

    def __init__(
        self, workload: IntegrationHubWorkloadBase, context: Context, config: CharmConfig
    ):
        self.workload = workload
        self.context = context
        self.config = config
        self.tls = TLSManager(context, workload)

    def _compare_and_update_file(self, content: str, file_path: str) -> bool:
        """Update the file at given file_path with given content.

        Before doing the update, compare the existing content of the file and update
        it only if has changed.

        Return True if the file was re-written, else False.
        """
        try:
            existing_content = "\n".join(self.workload.read(file_path))
            file_exists = True
        except FileNotFoundError:
            existing_content = ""
            file_exists = False
        self.logger.debug(f"{file_path=}")
        self.logger.debug(f"{existing_content=}")
        self.logger.debug(f"{content=}")

        is_content_different = set(existing_content.strip().splitlines()) != set(
            content.strip().splitlines()
        )
        if not file_exists or is_content_different:
            self.workload.write(content, file_path)
            return True

        return False

    def get_resource_manifest(
        self, namespace: str, username: str, configurations: dict[str, str]
    ) -> str:
        """Return the K8s resource manifest of the resources to be created."""
        self.logger.info("Generating manifest!")
        spark8t_manifest = self.workload.get_spark8t_manifest(
            namespace=namespace, username=username
        )
        hub_manifest = get_hub_secret_manifest(
            namespace=namespace, username=username, configurations=configurations
        )

        tls_manifest = ""
        if self.context.s3 and self.context.s3.tls_ca_chain:
            tls_manifest = get_hub_truststore_secret_manifest(
                namespace=namespace,
                truststore_filename=Path(self.context.cluster.truststore_path).name,
                truststore_content=self.workload.read_bytes(self.context.cluster.truststore_path),
                secret_name=self.context.cluster.truststore_secret_name,
            )
            self.logger.info(f"TLS manifest generated: {tls_manifest}")
        return "\n---\n".join(
            [
                manifest.strip()
                for manifest in (spark8t_manifest, hub_manifest, tls_manifest)
                if manifest
            ]
        )

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
        hub_conf = self.config

        self.logger.debug("Update")

        # update TLS configuration if needed. This is needed to be done before generating the config file since the presence of TLS configuration can impact the generated config (e.g., presence of truststore related properties in case of S3 with TLS).
        try:
            self.tls.reset()
        except Exception as e:
            self.logger.warning(f"Failed to reset truststore path: {e}.")
        finally:
            self.context.cluster.set_truststore_path("")
        if s3 and s3.tls_ca_chain:
            self.logger.info("Updating TLS configuration...")
            self.tls.import_ca("\n".join(s3.tls_ca_chain))

        config = IntegrationHubConfig(
            self.context, s3, azure_storage, pushgateway, hub_conf, loki_url
        )

        if any(
            [
                self._compare_and_update_file(
                    config.contents, str(self.workload.paths.spark_properties)
                ),
                self._compare_and_update_file(
                    "\n".join(
                        sorted(
                            [
                                *self.config.monitored_service_accounts,
                                *[
                                    sa.service_account
                                    for sa in self.context.service_accounts
                                    if sa.service_account
                                ],
                            ]
                        )
                    ),
                    str(self.workload.paths.allowlist),
                ),
            ]
        ):
            self.logger.info("Updating integration hub config...")

            self.workload.set_environment(
                {
                    "SPARK_PROPERTIES_FILE": str(self.workload.paths.spark_properties),
                    "SA_ALLOWLIST": str(self.workload.paths.allowlist),
                    "TRUSTSTORE_PATH": str(self.context.cluster.truststore_path),
                    "TRUSTSTORE_SECRET_NAME": str(self.context.cluster.truststore_secret_name),
                },
            )

            self.workload.restart()

        if self.context.charm.unit.is_leader():
            for sa in self.context.service_accounts:
                if not sa.service_account:
                    continue
                props = config.to_dict()
                sa.set_spark_properties(spark_properties=props)
                namespace, username = sa.service_account.split(":")
                manifest = self.get_resource_manifest(
                    namespace=namespace, username=username, configurations=props
                )
                sa.set_resource_manifest(manifest)
