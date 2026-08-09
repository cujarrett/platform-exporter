# platform-exporter

Prometheus exporter that watches Crossplane platform XRs, managed resources, and pods, and emits readiness and timing metrics — how long each kind takes to go from created to ready, which Apis are bound to which platform resources, and how long pod init containers take.

## Metrics

| Metric | Type | Labels | Description |
|---|---|---|---|
| `platform_xr_time_to_ready_seconds` | Histogram | `kind`, `backend` | Seconds from XR creation to `Ready=True`, aggregated by kind |
| `platform_xr_time_to_ready_seconds_instance` | Gauge | `kind`, `name`, `namespace`, `backend` | Seconds from XR creation to `Ready=True`, per instance |
| `platform_xr_ready` | Gauge | `kind`, `name`, `namespace`, `backend` | `1` if ready, `0` if not |
| `platform_xr_binding` | Gauge | `consumer_kind`, `consumer_name`, `binding_type`, `provider_name` | `1` when an Api has an active binding to a named platform resource |
| `platform_managed_time_to_ready_seconds` | Histogram | `kind` | Seconds from managed resource creation to `Ready=True`, aggregated by kind |
| `platform_managed_time_to_ready_seconds_instance` | Gauge | `kind`, `name`, `namespace` | Seconds from managed resource creation to `Ready=True`, per instance |
| `platform_managed_ready` | Gauge | `kind`, `name`, `namespace` | `1` if the managed resource is ready, `0` if not |
| `platform_pod_init_container_seconds` | Histogram | `init_container`, `namespace` | Seconds from pod creation to each init container completing, aggregated by namespace |
| `platform_pod_init_container_seconds_instance` | Gauge | `init_container`, `namespace`, `pod` | Seconds from pod creation to each init container completing, per pod |
| `platform_pod_time_to_ready_seconds` | Histogram | `namespace` | Seconds from pod creation to `Ready=True`, aggregated by namespace |
| `platform_pod_time_to_ready_seconds_instance` | Gauge | `namespace`, `pod` | Seconds from pod creation to `Ready=True`, per pod |

### Watched resources

**XRs** (all `platform.local.lab/v1alpha1`): `Api`, `Spa`, `Sql`, `NoSql`, `ObjectStorage`, `Cache`, `Topic`, `Subscription`

**Managed resources**:

| Kind | API |
|---|---|
| `DynamoDBTable` | `dynamodb.aws.upbound.io/v1beta1` |
| `ElastiCacheReplicationGroup` | `elasticache.aws.upbound.io/v1beta2` |
| `ElastiCacheUser` | `elasticache.aws.upbound.io/v1beta1` |
| `ElastiCacheUserGroup` | `elasticache.aws.upbound.io/v1beta1` |
| `IAMRole` | `iam.aws.upbound.io/v1beta1` |
| `NATSConsumer` | `jetstream.nats.io/v1beta2` |
| `NATSStream` | `jetstream.nats.io/v1beta2` |
| `RDSInstance` | `rds.aws.upbound.io/v1beta3` |
| `RolesAnywhereProfile` | `rolesanywhere.aws.upbound.io/v1beta1` |
| `S3Bucket` | `s3.aws.upbound.io/v1beta2` |

**Pods**: pods labelled `app in (api, spa)` — init container durations and pod ready time

### Api binding types

`platform_xr_binding` tracks refs from an Api's spec to other platform resources:

| `binding_type` | Spec field |
|---|---|
| `sql` | `spec.parameters.sqlRef.name` |
| `nosql` | `spec.parameters.nosqlRef.name` |
| `topic` | `spec.parameters.topicRef.name` |
| `subscription` | `spec.parameters.subscriptionRef.name` |
| `object-storage` | `spec.parameters.objectStorageRefs[].name` |

## Commands

| Command | What it does |
|---|---|
| `just ci` | Lint + test + build (run before pushing) |
| `just run` | Start locally on port 8080 (requires `KUBECONFIG`) |
| `just test` | Run tests with race detector |
| `just lint` | `go mod tidy -diff` + golangci-lint |

## Environment variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `PORT` | no | `8080` | Port to serve `/metrics` and `/healthz` on |
| `KUBECONFIG` | no | — | Path to kubeconfig for local dev. Uses in-cluster config when unset. |

## Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/metrics` | Prometheus metrics |
| `GET` | `/healthz` | Liveness probe |

## Deployment

Runs on the homelab cluster as a cluster-scoped service (not an Api — needs a ClusterRole to watch XRs across all namespaces). Manifests live in [`homelab/cluster/platform-exporter/`](https://github.com/cujarrett/homelab/tree/main/cluster/platform-exporter). Image: `ghcr.io/cujarrett/platform-exporter`. ARM64.

### Rotating `HOMELAB_PAT`

The `deploy` job authenticates to `cujarrett/homelab` with `HOMELAB_PAT`, a repo-level Actions secret holding a fine-grained PAT. This repo's token is its own — other repos also define a secret by that name, but theirs is scoped to `cujarrett/homelab-workspaces` and rotating one does not affect the other.

```bash
# 1. Mint a replacement at https://github.com/settings/personal-access-tokens
#    Repository access — cujarrett/homelab only
#    Permissions — Contents: Read and write

# 2. Replace the secret
print -n "Paste new token: "
read -rs NEW_TOKEN
echo
echo -n "$NEW_TOKEN" | gh secret set HOMELAB_PAT --repo cujarrett/platform-exporter --body -
unset NEW_TOKEN

# 3. Revoke the old token, then confirm the next merge to main still deploys.
#    CI has no workflow_dispatch trigger, so watch the run the next push produces.
gh run watch --repo cujarrett/platform-exporter \
  "$(gh run list --repo cujarrett/platform-exporter --branch main --limit 1 --json databaseId --jq '.[0].databaseId')"
```
