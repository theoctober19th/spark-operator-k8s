#!/usr/bin/env python3
# Copyright 2026 Canonical Data Platform
# See LICENSE file for licensing details.

"""Kubernetes resources that the Spark Operator needs and that the charm manages.

Two groups of resources are managed:

- The operator CRDs, which are cluster-scoped and large (hundreds of KiB each), hence they are
  applied server-side and only when they are missing or outdated.
- A service account, with the permissions that a Spark driver needs to create executor pods, in
  every namespace where the operator runs Spark applications.
"""

import hashlib
import logging
from functools import cached_property
from pathlib import Path

from lightkube.codecs import load_all_yaml
from lightkube.core.client import Client
from lightkube.core.exceptions import ApiError
from lightkube.models import authorization_v1
from lightkube.models.meta_v1 import ObjectMeta
from lightkube.models.rbac_v1 import PolicyRule, RoleRef, Subject
from lightkube.resources.apiextensions_v1 import CustomResourceDefinition
from lightkube.resources.authorization_v1 import SelfSubjectAccessReview
from lightkube.resources.core_v1 import ServiceAccount
from lightkube.resources.rbac_authorization_v1 import Role, RoleBinding

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

# Service account that Spark drivers use. Reference it in `.spec.driver.serviceAccount`.
JOB_SERVICE_ACCOUNT = "spark"
JOB_ROLE = "spark-driver"
JOB_ROLE_BINDING = "spark-driver"


class KubernetesResources:
    """Manage the Kubernetes resources of the Spark Operator."""

    def __init__(self, app_name: str):
        self.app_name = app_name

    @cached_property
    def client(self) -> Client:
        """Return the lightkube client."""
        return Client(field_manager=self.app_name)

    @property
    def _labels(self) -> dict[str, str]:
        return {"app.kubernetes.io/managed-by": self.app_name}

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
        """Install or update the CRDs, unless they are already up to date."""
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

    def apply_job_rbac(self, namespaces: list[str]) -> None:
        """Create the service account that Spark drivers use in the given namespaces."""
        for namespace in namespaces:
            logger.info("Applying Spark driver service account in namespace %s", namespace)
            meta = {"namespace": namespace, "labels": self._labels}
            self.client.apply(
                ServiceAccount(metadata=ObjectMeta(name=JOB_SERVICE_ACCOUNT, **meta)),
                force=True,
            )
            self.client.apply(
                Role(
                    metadata=ObjectMeta(name=JOB_ROLE, **meta),
                    rules=[
                        PolicyRule(
                            apiGroups=[""],
                            resources=["pods", "configmaps", "persistentvolumeclaims", "services"],
                            verbs=[
                                "get",
                                "list",
                                "watch",
                                "create",
                                "update",
                                "patch",
                                "delete",
                                "deletecollection",
                            ],
                        )
                    ],
                ),
                force=True,
            )
            self.client.apply(
                RoleBinding(
                    metadata=ObjectMeta(name=JOB_ROLE_BINDING, **meta),
                    roleRef=RoleRef(
                        apiGroup="rbac.authorization.k8s.io", kind="Role", name=JOB_ROLE
                    ),
                    subjects=[
                        Subject(kind="ServiceAccount", name=JOB_SERVICE_ACCOUNT, namespace=namespace)
                    ],
                ),
                force=True,
            )

    def remove_job_rbac(self, namespaces: list[str]) -> None:
        """Delete the service account that Spark drivers use from the given namespaces.

        The CRDs are deliberately kept: removing them would delete every Spark application.
        """
        for namespace in namespaces:
            for resource, name in (
                (RoleBinding, JOB_ROLE_BINDING),
                (Role, JOB_ROLE),
                (ServiceAccount, JOB_SERVICE_ACCOUNT),
            ):
                try:
                    self.client.delete(resource, name, namespace=namespace)
                except ApiError as e:
                    if e.status.code != 404:
                        raise
