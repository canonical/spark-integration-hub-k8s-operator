#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""S3 manager."""

import os
import tempfile
from functools import cached_property

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError, SSLError

from common.utils import WithLogging, is_proxy_skipped
from core.domain import S3ConnectionInfo


class S3Manager(WithLogging):
    """Class exposing business logic for interacting with S3 service."""

    def __init__(self, connection_info: S3ConnectionInfo):
        self.connection_info = connection_info

    @cached_property
    def session(self):
        """Return the S3 session to be used when connecting to S3."""
        return boto3.session.Session(
            aws_access_key_id=self.connection_info.access_key,
            aws_secret_access_key=self.connection_info.secret_key,
        )

    def _get_proxy_config(self) -> dict[str, str]:
        """Return proxy configuration based on charm environment variables."""
        proxy_config: dict[str, str] = {}
        if not is_proxy_skipped(self.connection_info.endpoint or ""):
            if os.environ.get("JUJU_CHARM_HTTPS_PROXY"):
                proxy_config["https"] = os.environ["JUJU_CHARM_HTTPS_PROXY"]
            if os.environ.get("JUJU_CHARM_HTTP_PROXY"):
                proxy_config["http"] = os.environ["JUJU_CHARM_HTTP_PROXY"]
        return proxy_config

    def _try_create_bucket(self, s3) -> bool:
        """Try to create the S3 bucket. Returns True on success, False on failure."""
        try:
            region = self.connection_info.region or "us-east-1"
            if region == "us-east-1":
                s3.create_bucket(Bucket=self.connection_info.bucket)
            else:
                s3.create_bucket(
                    Bucket=self.connection_info.bucket,
                    CreateBucketConfiguration={"LocationConstraint": region},
                )
            return True
        except ClientError as create_error:
            self.logger.error(f"Failed to create bucket... {create_error}")
            return False

    def verify(self) -> bool:
        """Verify S3 credentials."""
        with tempfile.NamedTemporaryFile() as ca_file:
            if tls_ca_chain := self.connection_info.tls_ca_chain:
                ca_file.write("\n".join(tls_ca_chain).encode())
                ca_file.flush()

            s3 = self.session.client(
                "s3",
                region_name=self.connection_info.region or "us-east-1",
                endpoint_url=self.connection_info.endpoint or "https://s3.amazonaws.com",
                verify=ca_file.name if self.connection_info.tls_ca_chain else None,
                config=Config(
                    request_checksum_calculation="when_supported",
                    response_checksum_validation="when_supported",
                    proxies=self._get_proxy_config(),
                ),
            )

            for attempt in range(2):
                try:
                    s3.list_objects_v2(
                        Bucket=self.connection_info.bucket,
                        Prefix=f"{self.connection_info.path}/",
                        MaxKeys=1,
                    )
                    s3.put_object(
                        Bucket=self.connection_info.bucket,
                        Key=f"{self.connection_info.path}/",
                        Body=b"",
                    )
                    return True
                except ClientError as client_error:
                    error_code = client_error.response["Error"]["Code"]
                    if error_code == "NoSuchBucket":
                        if not self._try_create_bucket(s3):
                            return False
                        continue
                    elif error_code == "PermanentRedirect":
                        self.logger.error(
                            f"S3 endpoint/region mismatch: bucket exists in a different region. "
                            f"Update the endpoint or region configuration. {client_error}"
                        )
                    else:
                        self.logger.error(f"Invalid S3 credentials... {client_error}")
                    return False
                except SSLError as ssl_error:
                    self.logger.error(f"SSL validation failed... {ssl_error}")
                    return False
                except Exception as e:
                    self.logger.error(f"S3 related error {e}")
                    return False
            return True
