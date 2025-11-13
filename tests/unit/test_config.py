# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

from pathlib import Path
from typing import Any

import pytest
import yaml
from pydantic import ValidationError

from core.config import CharmConfig

CONFIG = yaml.safe_load(Path("./config.yaml").read_text())


def check_valid_values(field: str, value: Any) -> None:
    """Check the correctness of the passed values for a field."""
    flat_config_options = {
        option_name.replace("-", "_"): mapping.get("default")
        for option_name, mapping in CONFIG["options"].items()
    }
    CharmConfig(**{**flat_config_options, **{field: value}})


def check_invalid_values(field: str, value: Any) -> None:
    """Check the incorrectness of the passed values for a field."""
    flat_config_options = {
        option_name.replace("-", "_"): mapping.get("default")
        for option_name, mapping in CONFIG["options"].items()
    }
    with pytest.raises(ValidationError) as excinfo:
        CharmConfig(**{**flat_config_options, **{field: value}})
    assert field in excinfo.value.errors()[0]["loc"]


@pytest.mark.parametrize(
    "value", ["", "foo:bar", "foo:bar,sa-1:sa-2", "foo:*,*:sa2", "foo:bar-*,*-sa1:sa2", "*:*"]
)
def test_correct_namespaces_service_accounts(value: str) -> None:
    # Given
    # When
    # Then
    check_valid_values("monitored_service_accounts", value)


@pytest.mark.parametrize(
    "value",
    [
        ","  # coma
        "foo:bar,",  # trailing coma
        ",foo:bar",  # leading coma
        " ",  # space
        "foo:bar ",  # tailing space
        " foo:bar",  # leading space
        "foo:",  # missing sa
        ":bar",  # missing ns
        "foo:barsa1:sa2",  # missing separator
    ],
)
def test_incorrect_namespaces_service_accounts(value: str) -> None:
    # Given
    # When
    # Then
    check_invalid_values("monitored_service_accounts", value)
