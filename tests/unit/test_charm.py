# Copyright 2026 Canonical Data Platform
# See LICENSE file for licensing details.

from unittest.mock import MagicMock

import ops
import pytest
from ops import testing

import spark_operator
from charm import SparkOperatorK8SCharm

MODEL = "spark-ns"


@pytest.fixture
def k8s(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    """Replace the Kubernetes client wrapper by a mock of a trusted app with CRDs installed."""
    mock = MagicMock()
    mock.trusted.return_value = True
    mock.crds_installed.return_value = True
    monkeypatch.setattr("charm.KubernetesResources", lambda app_name: mock)
    return mock


@pytest.fixture
def ctx() -> testing.Context:
    return testing.Context(SparkOperatorK8SCharm)


def make_container(can_connect: bool = True, ready: bool = True) -> testing.Container:
    status = ops.pebble.CheckStatus.UP if ready else ops.pebble.CheckStatus.DOWN
    return testing.Container(
        spark_operator.CONTAINER_NAME,
        can_connect=can_connect,
        check_infos={
            testing.CheckInfo(
                spark_operator.READY_CHECK_NAME,
                level=ops.pebble.CheckLevel.READY,
                status=status,
            )
        },
    )


def make_state(container: testing.Container | None = None, **kwargs) -> testing.State:
    kwargs.setdefault("leader", True)
    return testing.State(
        containers={container or make_container()}, model=testing.Model(name=MODEL), **kwargs
    )


def service_command(state: testing.State) -> str:
    container = state.get_container(spark_operator.CONTAINER_NAME)
    return container.plan.services[spark_operator.SERVICE_NAME].command


def test_leader_installs_resources_and_starts_controller(ctx, k8s):
    container = make_container()
    state = ctx.run(ctx.on.pebble_ready(container), make_state(container))

    k8s.apply_crds.assert_called_once_with()
    command = service_command(state)
    assert command.startswith(f"{spark_operator.BINARY} controller start ")
    assert f"--namespaces={MODEL}" in command
    assert f"--leader-election-lock-namespace={MODEL}" in command
    container_out = state.get_container(spark_operator.CONTAINER_NAME)
    assert (
        container_out.service_statuses[spark_operator.SERVICE_NAME]
        == ops.pebble.ServiceStatus.ACTIVE
    )
    assert state.unit_status == testing.ActiveStatus()


def test_non_leader_only_runs_the_controller(ctx, k8s):
    container = make_container()
    state = ctx.run(ctx.on.pebble_ready(container), make_state(container, leader=False))

    k8s.apply_crds.assert_not_called()
    assert spark_operator.SERVICE_NAME in state.get_container("controller").plan.services


def test_controller_waits_for_crds(ctx, k8s):
    k8s.crds_installed.return_value = False
    container = make_container()
    state = ctx.run(ctx.on.pebble_ready(container), make_state(container, leader=False))

    assert spark_operator.SERVICE_NAME not in state.get_container("controller").plan.services
    assert state.unit_status == testing.WaitingStatus("waiting for the leader to install the CRDs")


def test_blocked_when_not_trusted(ctx, k8s):
    k8s.trusted.return_value = False
    container = make_container()
    state = ctx.run(ctx.on.pebble_ready(container), make_state(container))

    k8s.apply_crds.assert_not_called()
    assert isinstance(state.unit_status, testing.BlockedStatus)
    assert "juju trust" in state.unit_status.message


def test_waits_while_controller_is_not_ready(ctx, k8s):
    container = make_container(ready=False)
    state = ctx.run(ctx.on.pebble_ready(container), make_state(container))

    assert state.unit_status == testing.WaitingStatus("waiting for the controller to be ready")


def test_blocked_on_invalid_log_level(ctx, k8s):
    state = ctx.run(
        ctx.on.config_changed(), make_state(config={"log-level": "chatty"})
    )

    k8s.apply_crds.assert_not_called()
    assert isinstance(state.unit_status, testing.BlockedStatus)


@pytest.mark.parametrize(
    ("job_namespaces", "flag"),
    [
        ("analytics, etl", "--namespaces=analytics,etl"),
        ("*", None),
    ],
)
def test_job_namespaces(ctx, k8s, job_namespaces, flag):
    container = make_container()
    state = ctx.run(
        ctx.on.pebble_ready(container),
        make_state(container, config={"job-namespaces": job_namespaces}),
    )

    command = service_command(state)
    if flag:
        assert flag in command
    else:
        assert "--namespaces" not in command


@pytest.mark.parametrize("enabled", [True, False])
def test_spark8t_submitter(ctx, k8s, enabled):
    container = make_container()
    state = ctx.run(
        ctx.on.pebble_ready(container),
        make_state(container, config={"spark8t-submitter": enabled}),
    )

    assert ("--feature-gates=Spark8tSubmitter=true" in service_command(state)) is enabled


def test_parse_job_namespaces():
    assert spark_operator.parse_job_namespaces("", "default") == ["default"]
    assert spark_operator.parse_job_namespaces("a,a, b", "default") == ["a", "b"]
    assert spark_operator.parse_job_namespaces("a,*", "default") is None
