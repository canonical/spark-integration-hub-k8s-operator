#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Manager for handling Spark History Server TLS configuration."""

import secrets
import string
import subprocess

from ops.pebble import ExecError

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
            certificate: string representing the certificate
        """
        self.workload.write(certificate, str(self.workload.paths.cert))

        command = [
            self.workload.paths.keytool,
            "-import",
            "-v",
            "-alias",
            "ca",
            "-file",
            str(self.workload.paths.cert),
            "-keystore",
            str(self.workload.paths.truststore),
            "-storepass",
            self.truststore_password(),
            "-noprompt",
        ]

        try:
            self.workload.exec(command=command, working_dir=str(self.workload.paths.conf_path))
            self.workload.exec(
                [
                    "chown",
                    "-R",
                    f"{self.workload.user.name}:{self.workload.user.group}",
                    str(self.workload.paths.truststore),
                ]
            )
            self.workload.exec(["chmod", "-R", "660", str(self.workload.paths.truststore)])
            self.context.cluster.set_truststore_path(str(self.workload.paths.truststore))
            self.logger.info("Certificate imported to truststore successfully")

        except (subprocess.CalledProcessError, ExecError) as e:
            # in case this reruns and fails
            if e.stdout and "already exists" in e.stdout:
                return
            self.logger.error(e.stdout)
            raise e

    def reset(self):
        """Remove all files related to TLS configuration."""
        self.logger.info("Deleting TLS files...")
        self.workload.exec(["rm", "-f", str(self.workload.paths.truststore)])
        self.workload.exec(["rm", "-f", str(self.workload.paths.cert)])
        self.context.cluster.set_truststore_path("")
