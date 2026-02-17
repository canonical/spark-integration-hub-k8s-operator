#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""S3 manager."""

import tempfile
from functools import cached_property

import boto3
from botocore.exceptions import ClientError, SSLError

from common.utils import WithLogging
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

    def verify(self) -> bool:
        """Verify S3 credentials."""
        with tempfile.NamedTemporaryFile() as ca_file:
            if config := self.connection_info.tls_ca_chain:
                ca_file.write("\n".join(config).encode())
                ca_file.flush()

            s3 = self.session.client(
                "s3",
                region_name=self.connection_info.region or "us-east-1",
                endpoint_url=self.connection_info.endpoint or "https://s3.amazonaws.com",
                verify=ca_file.name if self.connection_info.tls_ca_chain else None,
            )

            try:
                s3.list_buckets()
            except ClientError as client_error:
                self.logger.error(f"Invalid S3 credentials... {client_error}")
                return False
            except SSLError as ssl_error:
                self.logger.error(f"SSL validation failed... {ssl_error}")
                return False
            except Exception as e:
                self.logger.error(f"S3 related error {e}")
                return False

        return True
