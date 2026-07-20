# platform-exporter

Go Prometheus exporter that watches Crossplane platform XRs, managed resources, and pods, and emits readiness, timing, and binding metrics. Single binary, no frameworks.

## Rules

- **Never run `git commit`, `git push`, or any git command that writes to or modifies repository history or remotes.** If a task requires committing or pushing, stop and tell the user to run the git command manually.

### Pre-commit safety check

Before telling the user to commit, always run `/pre-commit-review`. It checks for secrets, sensitive identifiers, PII, credential templates, and cluster safety, and returns explicit verdicts on whether the changes are safe for a public repo. Once it confirms the changes are safe, offer the user a suggested commit message — do not run `git commit` yourself.

## Commands
| Command | What it does |
|---|---|
| `just ci` | Lint + test + build (run before pushing) |
| `just run` | Start locally (requires KUBECONFIG) |
| `just test` | Run tests with race detector |
| `just lint` | go mod tidy -diff + golangci-lint |

## Architecture
- `main.go` — entrypoint, k8s client setup, metrics HTTP server on :8080
- `watcher.go` — dynamic watch loop per XR and managed resource kind; binding extraction and sync; condition parsing
- `pod_watcher.go` — pod watch loop for init container durations and pod ready time
- `metrics.go` — Prometheus histogram/gauge definitions

## Watched resources

**XRs** (`platform.local.lab/v1alpha1`): Api, Spa, Sql, NoSql, ObjectStorage, Cache, Topic, Subscription

**Managed resources**: IAMRole (`iam.aws.upbound.io`), RolesAnywhereProfile (`rolesanywhere.aws.upbound.io`)

**Pods**: labelled `app in (api, spa)` — init container durations and pod ready time

## Metrics
- `platform_xr_time_to_ready_seconds{kind, backend}` — histogram, creation → Ready=True
- `platform_xr_ready{kind, name, namespace, backend}` — gauge, 1=ready 0=not
- `platform_xr_binding{consumer_kind, consumer_name, binding_type, provider_name}` — gauge, 1=active binding; tracks sqlRef, nosqlRef, topicRef, subscriptionRef, objectStorageRefs on Api; cleared on Api deletion
- `platform_managed_time_to_ready_seconds{kind}` — histogram, creation → Ready=True
- `platform_managed_ready{kind, name, namespace}` — gauge, 1=ready 0=not
- `platform_pod_init_container_seconds{init_container, namespace}` — histogram, pod creation → init container finished
- `platform_pod_time_to_ready_seconds{namespace}` — histogram, pod creation → Ready=True

## Conventions
- No frameworks — stdlib + client-go + prometheus/client_golang
- `slog` structured logging (JSON)
- Graceful shutdown via signal.NotifyContext
- KUBECONFIG env for local dev, in-cluster config in production
