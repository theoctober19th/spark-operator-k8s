#!/usr/bin/env python3
# Copyright 2026 Canonical Data Platform
# See LICENSE file for licensing details.

"""The operator CRDs, which the charm manages.

The CRDs are cluster-scoped and large (hundreds of KiB each), hence they are applied server-side
and only when they are missing or outdated.
"""

import hashlib
import logging
from functools import cached_property
from pathlib import Path

from lightkube.codecs import load_all_yaml
from lightkube.core.client import Client
from lightkube.core.exceptions import ApiError
from lightkube.models import authorization_v1
from lightkube.resources.apiextensions_v1 import CustomResourceDefinition
from lightkube.resources.authorization_v1 import SelfSubjectAccessReview

logger = logging.getLogger(__name__)

CRD_DIR = Path(__file__).parent / "manifests" / "crds"

# CRD name -> file that defines it.
CRD_FILES = {
    "sparkapplications.sparkoperator.k8s.io": "sparkoperator.k8s.io_sparkapplications.yaml",
    "scheduledsparkapplications.sparkoperator.k8s.io": (
        "sparkoperator.k8s.io_scheduledsparkapplications.yaml"
    ),
    "sparkconnects.sparkoperator.k8s.io": "sparkoperator.k8s.io_sparkconnects.yaml",
}
CRD_HASH_ANNOTATION = "spark-operator-k8s.charm/manifest-sha256"


class KubernetesResources:
    """Manage the Kubernetes resources of the Spark Operator."""

    def __init__(self, app_name: str):
        self.app_name = app_name

    @cached_property
    def client(self) -> Client:
        """Return the lightkube client."""
        return Client(field_manager=self.app_name)

    def trusted(self) -> bool:
        """Check whether the application can manage CRDs, which requires `juju trust`."""
        review = SelfSubjectAccessReview(
            spec=authorization_v1.SelfSubjectAccessReviewSpec(
                resourceAttributes=authorization_v1.ResourceAttributes(
                    group="apiextensions.k8s.io",
                    resource="customresourcedefinitions",
                    verb="create",
                )
            )
        )
        try:
            return bool(self.client.create(review).status.allowed)  # type: ignore[union-attr]
        except ApiError:
            logger.exception("Could not check whether the application is trusted")
            return False

    @staticmethod
    def _manifest_hash(file_name: str) -> str:
        return hashlib.sha256((CRD_DIR / file_name).read_bytes()).hexdigest()

    def crds_installed(self) -> bool:
        """Check whether all the CRDs exist."""
        for name in CRD_FILES:
            try:
                self.client.get(CustomResourceDefinition, name)
            except ApiError as e:
                if e.status.code == 404:
                    return False
                raise
        return True

    def crds_up_to_date(self) -> bool:
        """Check whether all the CRDs exist and match the ones vendored in the charm."""
        for name, file_name in CRD_FILES.items():
            try:
                crd = self.client.get(CustomResourceDefinition, name)
            except ApiError as e:
                if e.status.code == 404:
                    return False
                raise
            annotations = (crd.metadata.annotations if crd.metadata else None) or {}
            if annotations.get(CRD_HASH_ANNOTATION) != self._manifest_hash(file_name):
                return False
        return True

    def apply_crds(self) -> None:
        """Install or update the CRDs, unless they are already up to date.

        The CRDs are never deleted by the charm: removing them would delete every Spark
        application.
        """
        if self.crds_up_to_date():
            return
        for name, file_name in CRD_FILES.items():
            logger.info("Applying CRD %s", name)
            with open(CRD_DIR / file_name) as manifest:
                crds = load_all_yaml(manifest)
            for crd in crds:
                assert isinstance(crd, CustomResourceDefinition) and crd.metadata
                crd.metadata.annotations = {
                    **(crd.metadata.annotations or {}),
                    CRD_HASH_ANNOTATION: self._manifest_hash(file_name),
                }
                # Forcing ownership is needed for CRDs that were installed with `kubectl` or Helm.
                self.client.apply(crd, force=True)
