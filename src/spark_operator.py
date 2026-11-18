#!/usr/bin/env python3
# Copyright 2026 Canonical Data Platform
# See LICENSE file for licensing details.

"""Workload-specific logic for the Kubeflow Spark Operator controller.

This module has no charming concerns, so that it can be tested and reasoned about on its own.
"""

import ops

CONTAINER_NAME = "controller"
SERVICE_NAME = "controller"
BINARY = "/usr/bin/spark-operator"

HEALTH_PORT = 8081
READY_CHECK_NAME = "controller-ready"
ALIVE_CHECK_NAME = "controller-alive"

# Namespace value that makes the operator watch every namespace of the cluster.
ALL_NAMESPACES = "*"


def parse_job_namespaces(value: str, default_namespace: str) -> list[str] | None:
    """Return the namespaces the operator should watch.

    Returns None when the whole cluster should be watched, and the list of namespaces otherwise.
    """
    entries = [entry.strip() for entry in value.split(",") if entry.strip()]
    if not entries:
        return [default_namespace]
    if ALL_NAMESPACES in entries:
        return None
    return list(dict.fromkeys(entries))


def build_command(
    *,
    app_name: str,
    lock_namespace: str,
    namespaces: list[str] | None,
    log_level: str,
    controller_threads: int,
) -> str:
    """Return the command line that starts the controller."""
    args = [
        BINARY,
        "controller",
        "start",
        f"--zap-log-level={log_level}",
        f"--controller-threads={controller_threads}",
        f"--health-probe-bind-address=:{HEALTH_PORT}",
        "--leader-election=true",
        f"--leader-election-lock-name={app_name}-controller-lock",
        f"--leader-election-lock-namespace={lock_namespace}",
    ]
    if namespaces is not None:
        args.append(f"--namespaces={','.join(namespaces)}")
    return " ".join(args)


def build_layer(command: str) -> ops.pebble.LayerDict:
    """Return the Pebble layer that runs the controller and checks its health."""
    health_url = f"http://localhost:{HEALTH_PORT}"
    return {
        "summary": "Kubeflow Spark Operator controller",
        "services": {
            SERVICE_NAME: {
                "override": "replace",
                "summary": "Kubeflow Spark Operator controller",
                "command": command,
                "startup": "enabled",
                "on-failure": "restart",
            }
        },
        "checks": {
            ALIVE_CHECK_NAME: {
                "override": "replace",
                "level": "alive",
                "http": {"url": f"{health_url}/healthz"},
            },
            READY_CHECK_NAME: {
                "override": "replace",
                "level": "ready",
                "http": {"url": f"{health_url}/readyz"},
            },
        },
    }
