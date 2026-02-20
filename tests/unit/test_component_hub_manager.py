# Copyright 2026 Canonical Limited
# See LICENSE file for licensing details

from dataclasses import dataclass
from unittest import mock

from pytest import MonkeyPatch

from managers.integration_hub import IntegrationHubConfig


@dataclass
class S3InfoTester:
    endpoint: str = "http://192.168.1.1"
    access_key: str = "foo"
    secret_key: str = "bar"
    bucket: str = "bucket"
    path: str = "path/"
    region: str = ""
    tls_ca_chain: str = ""
    log_dir: str = ""
    file_upload_path: str = ""
    warehouse_path: str = ""


def test_s3_proxy_credentials(monkeypatch: MonkeyPatch) -> None:
    """Proxy credentials are properly extracted and passed to the spark properties."""
    # Given
    monkeypatch.setenv("JUJU_CHARM_HTTP_PROXY", "http://username:password@10.152.193.234:80")

    with mock.patch("managers.integration_hub.S3Manager", mock.MagicMock()) as mocked_s3_manager:
        instance = mocked_s3_manager.return_value
        instance.connection_info = S3InfoTester()
        config = IntegrationHubConfig(object(), None, None, None, None)  # type: ignore

        # When
        s3_proxy_conf = config._s3_conf

    # Then
    assert s3_proxy_conf.get("spark.hadoop.fs.s3a.proxy.username", "") == "username"
    assert s3_proxy_conf.get("spark.hadoop.fs.s3a.proxy.password", "") == "password"


def test_s3_proxy_plain_ip(monkeypatch: MonkeyPatch) -> None:
    """Proper scheme and port are passed to the spark properties.

    Even if https_proxy is pointing to an http domain.
    """
    # Given
    proxy_host = "10.152.193.234"
    monkeypatch.setenv("JUJU_CHARM_HTTP_PROXY", f"http://{proxy_host}")

    with mock.patch("managers.integration_hub.S3Manager", mock.MagicMock()) as mocked_s3_manager:
        instance = mocked_s3_manager.return_value
        instance.connection_info = S3InfoTester()
        config = IntegrationHubConfig(object(), None, None, None, None)  # type: ignore

        # When
        s3_proxy_conf = config._s3_conf

    # Then
    assert s3_proxy_conf.get("spark.hadoop.fs.s3a.proxy.host", "") == proxy_host
    assert s3_proxy_conf.get("spark.hadoop.fs.s3a.proxy.ssl.enabled", "") == "false"
    assert s3_proxy_conf.get("spark.hadoop.fs.s3a.proxy.port", "0") == "80"
