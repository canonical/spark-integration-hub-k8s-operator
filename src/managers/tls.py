#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Manager for handling Spark History Server TLS configuration."""

import secrets
import string

from cryptography import x509
from cryptography.hazmat.primitives.serialization import BestAvailableEncryption, pkcs12

from common.utils import WithLogging
from core.context import Context
from core.workload import IntegrationHubWorkloadBase


class TLSManager(WithLogging):
    """Manager for building necessary files for Java TLS auth."""

    def __init__(self, context: Context, workload: IntegrationHubWorkloadBase):
        self.context = context
        self.workload = workload

    @staticmethod
    def generate_password() -> str:
        """Creates randomized string for use as app passwords.

        Returns:
            String of 32 randomized letter+digit characters
        """
        return "".join([secrets.choice(string.ascii_letters + string.digits) for _ in range(32)])

    def truststore_password(self) -> str:
        """Return the password of the truststore."""
        if not self.context.cluster.truststore_password:
            self.logger.info("Generating new truststore password")
            password = self.generate_password()
            self.context.cluster.set_truststore_password(password)
            return password

        return self.context.cluster.truststore_password

    def import_ca(self, certificate: str):
        """Import a certificate into the truststore.

        Args:
            certificate: string representing the certificate (PEM, may be a chain)
        """
        self.workload.write(certificate, str(self.workload.paths.cert))

        # Build a PKCS12 truststore in-process; the JVM reads it via trustStoreType=PKCS12.
        certs = x509.load_pem_x509_certificates(certificate.encode())
        truststore = pkcs12.serialize_key_and_certificates(
            name=b"ca",
            key=None,
            cert=None,
            cas=certs,
            encryption_algorithm=BestAvailableEncryption(self.truststore_password().encode()),
        )
        self.workload.write(
            content=truststore,
            path=str(self.workload.paths.truststore),
        )
        self.context.cluster.set_truststore_path(str(self.workload.paths.truststore))
        self.logger.info("Certificate imported to truststore successfully")

    def reset(self):
        """Remove all files related to TLS configuration."""
        self.logger.info("Deleting TLS files...")
        self.workload.exec(["rm", "-f", str(self.workload.paths.truststore)])
        self.workload.exec(["rm", "-f", str(self.workload.paths.cert)])
        self.context.cluster.set_truststore_path("")
