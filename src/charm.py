#!/usr/bin/env python3
# Copyright 2026 Canonical Data Platform
# See LICENSE file for licensing details.

"""Charm for the Kubeflow Spark Operator."""

import logging
from functools import cached_property
from typing import cast

import ops
from charms.spark_integration_hub_k8s.v0.spark_service_account import (
    SparkServiceAccountRequirer,
)
from lightkube.core.exceptions import ApiError

import spark_operator
from kubernetes_resources import KubernetesResources

logger = logging.getLogger(__name__)

LOG_LEVELS = ("debug", "info", "error")
SERVICE_ACCOUNT_RELATION = "spark-service-account"


class SparkOperatorK8SCharm(ops.CharmBase):
    """Run the Kubeflow Spark Operator controller and manage its CRDs."""

    def __init__(self, framework: ops.Framework):
        super().__init__(framework)
        self.container = self.unit.get_container(spark_operator.CONTAINER_NAME)
        self.k8s = KubernetesResources(self.app.name)
        self._resource_error: str | None = None

        # The account is requested in the namespace of the model, where the Hub creates it.
        self.service_account_requirer = SparkServiceAccountRequirer(
            self,
            relation_name=SERVICE_ACCOUNT_RELATION,
            service_account=f"{self.model.name}:{self.config['service-account']}",
        )

        for event in (
            self.on.install,
            self.on.upgrade_charm,
            self.on.leader_elected,
            self.on.config_changed,
            self.on.update_status,
            self.on.controller_pebble_ready,
            self.on.controller_pebble_check_failed,
            self.on.controller_pebble_check_recovered,
        ):
            framework.observe(event, self._reconcile)
        for event in (
            self.service_account_requirer.on.account_granted,
            self.service_account_requirer.on.account_gone,
            self.service_account_requirer.on.properties_changed,
            self.on[SERVICE_ACCOUNT_RELATION].relation_changed,
            self.on[SERVICE_ACCOUNT_RELATION].relation_broken,
        ):
            framework.observe(event, self._reconcile)
        framework.observe(self.on.restart_action, self._on_restart_action)
        framework.observe(self.on.collect_unit_status, self._on_collect_status)

    @cached_property
    def _trusted(self) -> bool:
        return self.k8s.trusted()

    @property
    def _watched_namespaces(self) -> list[str] | None:
        """Namespaces watched by the operator, or None for the whole cluster."""
        value = cast(str, self.config["job-namespaces"])
        return spark_operator.parse_job_namespaces(value, default_namespace=self.model.name)

    @property
    def _hub_service_account(self) -> str | None:
        """Name of the service account granted by the Integration Hub, if any."""
        relation = self.model.get_relation(SERVICE_ACCOUNT_RELATION)
        if relation is None or relation.app is None or not relation.active:
            return None
        account = relation.data[relation.app].get("service-account", "")
        # The value is `<namespace>:<username>`; the namespace is the one of this model.
        return account.partition(":")[2] or None

    def _config_error(self) -> str | None:
        """Return a message describing the invalid configuration, if any."""
        if self.config["log-level"] not in LOG_LEVELS:
            return f"invalid log-level, use one of: {', '.join(LOG_LEVELS)}"
        if cast(int, self.config["controller-threads"]) < 1:
            return "controller-threads must be at least 1"
        return None

    def _reconcile(self, _: ops.EventBase) -> None:
        """Bring the CRDs and the workload to the desired state."""
        if self._config_error() or not self._trusted:
            return

        if self.unit.is_leader():
            try:
                self.k8s.apply_crds()
            except ApiError as e:
                logger.exception("Failed to apply the CRDs")
                self._resource_error = f"cannot apply the CRDs: {e.status.message}"
                return

        # The controller does not install the CRDs, and fails to start without them.
        if not self.container.can_connect() or not self.k8s.crds_installed():
            return

        command = spark_operator.build_command(
            app_name=self.app.name,
            lock_namespace=self.model.name,
            namespaces=self._watched_namespaces,
            log_level=cast(str, self.config["log-level"]),
            controller_threads=cast(int, self.config["controller-threads"]),
            spark8t_submitter=cast(bool, self.config["spark8t-submitter"]),
            default_service_account=self._hub_service_account,
        )
        self.container.add_layer(
            spark_operator.SERVICE_NAME, spark_operator.build_layer(command), combine=True
        )
        self.container.replan()

    def _on_restart_action(self, event: ops.ActionEvent) -> None:
        try:
            self.container.restart(spark_operator.SERVICE_NAME)
        except (ops.pebble.ConnectionError, ops.pebble.ChangeError, ops.ModelError) as e:
            event.fail(f"cannot restart the controller: {e}")

    def _workload_ready(self) -> bool:
        services = self.container.get_services(spark_operator.SERVICE_NAME)
        if not services or not all(service.is_running() for service in services.values()):
            return False
        checks = self.container.get_checks(level=ops.pebble.CheckLevel.READY)
        return all(check.status == ops.pebble.CheckStatus.UP for check in checks.values())

    def _on_collect_status(self, event: ops.CollectStatusEvent) -> None:
        if error := self._config_error():
            event.add_status(ops.BlockedStatus(error))
            return
        if not self._trusted:
            event.add_status(
                ops.BlockedStatus(f"run `juju trust {self.app.name} --scope=cluster`")
            )
            return
        if self._resource_error:
            event.add_status(ops.BlockedStatus(self._resource_error))
            return
        if not self.container.can_connect():
            event.add_status(ops.WaitingStatus("waiting for the controller container"))
            return
        if self.model.get_relation(SERVICE_ACCOUNT_RELATION) and not self._hub_service_account:
            event.add_status(
                ops.WaitingStatus("waiting for the service account from the Integration Hub")
            )
            return
        if not self.k8s.crds_installed():
            event.add_status(ops.WaitingStatus("waiting for the leader to install the CRDs"))
            return
        if not self._workload_ready():
            event.add_status(ops.WaitingStatus("waiting for the controller to be ready"))
            return
        event.add_status(ops.ActiveStatus())


if __name__ == "__main__":  # pragma: nocover
    ops.main(SparkOperatorK8SCharm)
