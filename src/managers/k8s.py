#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Kubernetes manager."""

import lightkube
from lightkube.core.client import Client
from lightkube.core.exceptions import ApiError
from lightkube.models import authorization_v1
from lightkube.resources.authorization_v1 import SelfSubjectAccessReview

from common.utils import WithLogging


class KubernetesManager(WithLogging):
    """Class exposing business logic for interacting with Kubernetes."""

    def __init__(self, app_name: str):
        self.app_name = app_name
        self.client = Client(field_manager=app_name)

    def trusted(self) -> bool:
        """Check whether application is trusted."""
        try:
            return getattr(
                lightkube.Client()
                .create(
                    SelfSubjectAccessReview(
                        spec=authorization_v1.SelfSubjectAccessReviewSpec(
                            resourceAttributes=authorization_v1.ResourceAttributes(
                                name=self.app_name,
                                namespace="test",
                                resource="statefulset",
                                verb="patch",
                            )
                        )
                    )
                )
                .status,
                "allowed",
                False,
            )
        except ApiError:
            return False
