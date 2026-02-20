#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Utilities."""

import ipaddress
import os
from logging import Logger, getLogger
from typing import Any, Callable, Literal, TypedDict, Union, cast
from urllib.parse import urlparse

from lightkube.codecs import AnyResource, dump_all_yaml
from lightkube.resources.core_v1 import Secret
from spark8t.literals import HUB_LABEL
from spark8t.utils import PercentEncodingSerializer

PathLike = Union[str, "os.PathLike[str]"]

LevelTypes = Literal[
    "CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "NOTSET", 50, 40, 30, 20, 10, 0
]
StrLevelTypes = Literal["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "NOTSET"]


class LevelsDict(TypedDict):
    """Log Levels."""

    CRITICAL: Literal[50]
    ERROR: Literal[40]
    WARNING: Literal[30]
    INFO: Literal[20]
    DEBUG: Literal[10]
    NOTSET: Literal[0]


DEFAULT_LOG_LEVEL: StrLevelTypes = "INFO"

levels: LevelsDict = {
    "CRITICAL": 50,
    "ERROR": 40,
    "WARNING": 30,
    "INFO": 20,
    "DEBUG": 10,
    "NOTSET": 0,
}


class WithLogging:
    """Base class to be used for providing a logger embedded in the class."""

    @property
    def logger(self) -> Logger:
        """Create logger.

        :return: default logger.
        """
        name_logger = str(self.__class__).replace("<class '", "").replace("'>", "")
        return getLogger(name_logger)

    def log_result(
        self, msg: Union[Callable[..., str], str], level: StrLevelTypes = "INFO"
    ) -> Callable[..., Any]:
        """Return a decorator to allow logging of inputs/outputs.

        :param msg: message to log
        :param level: logging level
        :return: wrapped method.
        """

        def wrap(x: Any) -> Any:
            if isinstance(msg, str):
                self.logger.log(levels[level], msg)
            else:
                self.logger.log(levels[level], msg(x))
            return x

        return wrap


def get_hub_secret_manifest(
    namespace: str, username: str, configurations: dict[str, str] | None
) -> str:
    """Return the K8s resource manifest corresponding to Hub configuration secret."""
    secret_name = f"{HUB_LABEL}-{username}"
    if configurations is None:
        configurations = {}
    serialized_config = {
        PercentEncodingSerializer().serialize(key): value for key, value in configurations.items()
    }
    secret = Secret.from_dict(
        {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {
                "name": secret_name,
                "namespace": namespace,
                "labels": {"app.kubernetes.io/generated-by": "integration-hub"},
            },
            "stringData": serialized_config,
        }
    )
    manifest = dump_all_yaml([cast(AnyResource, secret)]) or ""
    return manifest


def is_proxy_skipped(endpoint: str) -> bool:
    """Determine if proxy should not be applied for the given endpoint."""
    no_proxy_list = os.environ.get("JUJU_CHARM_NO_PROXY", "")
    if not no_proxy_list:
        return False

    host = urlparse(endpoint).hostname
    if not host:
        return False
    no_proxy_entries = [
        entry.strip().lower() for entry in no_proxy_list.split(",") if entry.strip()
    ]
    for entry in no_proxy_entries:
        if host == entry:
            return True
        elif entry.startswith(".") and host.endswith(
            entry
        ):  # abc.example.com matches .example.com
            return True
        elif host.endswith("." + entry):  # abc.example.com matches example.com
            return True
        try:
            if ipaddress.ip_address(host) in ipaddress.ip_network(
                entry, strict=False
            ):  # CIDR match
                return True
        except (AttributeError, ValueError):
            continue

    return False
