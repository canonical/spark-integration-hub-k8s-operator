#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Utilities."""

import os
from logging import Logger, getLogger
from typing import Any, Callable, Literal, TypedDict, Union

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


class DotSerializer:
    """A utility class that can serialize a string so as to remove the dot characters from it.

    Dots are replaced with underscores and underscores are converted to double underscores.
    """

    _SPECIAL_CHAR = "§"

    def __init__(self, replacement_char: str = "_"):
        self.replacement_char = replacement_char

    def serialize(self, input_string: str) -> str:
        """Serialize the string to remove dot characters."""
        return input_string.replace(self.replacement_char, self.replacement_char * 2).replace(
            ".", self.replacement_char
        )

    def deserialize(self, input_string: str) -> str:
        """Deserialize the string to original form."""
        return (
            input_string.replace(self.replacement_char * 2, self._SPECIAL_CHAR)
            .replace(self.replacement_char, ".")
            .replace(self._SPECIAL_CHAR, self.replacement_char)
        )
