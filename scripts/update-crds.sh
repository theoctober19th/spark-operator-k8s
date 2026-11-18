#!/usr/bin/env bash
# Copyright 2026 Canonical Data Platform
# See LICENSE file for licensing details.

# Vendor the Kubeflow Spark Operator CRDs for a given upstream release into the charm.
# Keep the version in line with the `upstream-source` of the controller image in charmcraft.yaml.
#
# Usage: scripts/update-crds.sh 2.6.0

set -euo pipefail

VERSION="${1:?usage: $0 <upstream-version, e.g. 2.6.0>}"
DEST="$(dirname "$0")/../src/manifests/crds"
BASE="https://raw.githubusercontent.com/kubeflow/spark-operator/v${VERSION}/charts/spark-operator-chart/crds"

mkdir -p "${DEST}"
for crd in sparkapplications scheduledsparkapplications sparkconnects; do
    curl --fail --silent --show-error --location \
        "${BASE}/sparkoperator.k8s.io_${crd}.yaml" \
        --output "${DEST}/sparkoperator.k8s.io_${crd}.yaml"
done

echo "Vendored Spark Operator CRDs v${VERSION} into ${DEST}"
