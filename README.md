# Kubeflow Spark Operator K8s charm (proof of concept)

A Juju charm that deploys the [Kubeflow Spark Operator](https://spark.kubeflow.org/en/latest/index.html),
so that Spark applications can be run declaratively with `SparkApplication` custom resources.

## What the charm does

- Runs the upstream controller (`spark-operator controller start`) from the upstream OCI image,
  under Pebble, with leader election so that the application can be scaled.
- Installs the operator CRDs (vendored in `src/manifests/crds`, see `scripts/update-crds.sh`).

## Not in scope yet

- Service accounts for Spark drivers. Create them yourself in the watched namespaces (see
  `examples/spark-rbac.yaml`).
- The admission webhook. Features that need it (mounting Secrets and ConfigMaps or volumes,
  affinity, tolerations, sidecars, ...) do not work yet.
- Least-privilege RBAC for the controller: the charm relies on `juju deploy --trust`.
- Relations (Integration Hub, object storage, COS, ...).

## Usage

```shell
charmcraft pack
juju deploy ./spark-operator-k8s_amd64.charm --trust \
  --resource controller-image=ghcr.io/kubeflow/spark-operator/controller:2.6.0
kubectl apply -f examples/spark-rbac.yaml   # edit the namespace to the Juju model name
kubectl apply -f examples/spark-pi.yaml     # likewise
kubectl -n <model> get sparkapplication spark-pi
```

## Configuration

| Option               | Default | Description                                                                        |
| -------------------- | ------- | ---------------------------------------------------------------------------------- |
| `job-namespaces`     | `""`    | Namespaces to watch, comma-separated. Empty: the model namespace. `*`: the cluster. |
| `controller-threads` | `10`    | Number of concurrent reconcile workers.                                            |
| `log-level`          | `info`  | `debug`, `info` or `error`.                                                        |

## Known limitations

- The CRDs are cluster-scoped and shared. The charm applies them with server-side apply and takes
  ownership of conflicting fields, which also affects any other installation of the operator
  (for instance a Helm release) in the same cluster. They are never deleted on removal, since
  that would delete every Spark application.
