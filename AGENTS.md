# platform-exporter

Go Prometheus exporter that watches Crossplane platform XRs, managed resources, and pods, and emits readiness, timing, and binding metrics. Single binary, no frameworks.

## Rules

- **Never run `git commit`, `git push`, or any git command that writes to or modifies repository history or remotes.** If a task requires committing or pushing, stop and tell the user to run the git command manually.
- **Never add a `Co-Authored-By` trailer or a "Generated with Claude Code" line** to commit messages or PR descriptions, including in suggested commit messages. Commits are authored by the user alone.
- **Cheapest rung that works.** Before writing code go down the ladder and stop at the first rung that solves it - skip the feature, reuse code already here, standard library, native platform feature, a dependency already installed, one line, then build the minimum.

### Pre-commit safety check

Before telling the user to commit, always run `/security-review`. It reviews the pending changes on the current branch for security issues. Once it confirms the changes are safe, offer the user a suggested commit message - do not run `git commit` yourself.

## Commands
| Command | What it does |
|---|---|
| `just ci` | Lint + test + build (run before pushing) |
| `just run` | Start locally (requires KUBECONFIG) |
| `just test` | Run tests with race detector |
| `just lint` | go mod tidy -diff + golangci-lint |

## Architecture
- `main.go`: entrypoint, k8s client setup, metrics HTTP server on :8080
- `watcher.go`: dynamic watch loop per XR and managed resource kind, binding extraction and sync, condition parsing
- `pod_watcher.go`: pod watch loop for init container durations and pod ready time
- `metrics.go`: Prometheus histogram and gauge definitions

## Watched resources

The code is the list. Add a kind there and nowhere else.

- **XRs**: `watchedXRs` in `watcher.go`
- **Managed resources**: `watchedManaged` in `watcher.go`
- **Pods**: labelled `app in (api,spa)`, set in `pod_watcher.go`

## Metrics

`metrics.go` defines them, all under the `platform_` prefix. [README → Metrics](./README.md#metrics) has every name, type and label.

- Each timing has a histogram by kind or namespace, and an `_instance` gauge per object.
- `platform_xr_binding` tracks an Api's refs to other platform resources. It clears when the Api is deleted.

## Conventions
- No frameworks. Stdlib, client-go and prometheus/client_golang
- `slog` structured logging (JSON)
- Graceful shutdown via signal.NotifyContext
- KUBECONFIG env for local dev, in-cluster config in production
