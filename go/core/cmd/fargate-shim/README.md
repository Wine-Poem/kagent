# fargate-shim

Runs kagent on EKS Fargate without Agent Substrate. The shim serves the part of the
ate-api `Control` gRPC service that the kagent controller uses and turns every Actor
into one Fargate pod, so each Session gets its own Firecracker microVM. It also
replaces the Atenet router: a request for a suspended Actor starts its pod first.

This is a feasibility probe, not a supported component.

## How the Substrate concepts map

| Substrate | fargate-shim |
|---|---|
| ActorTemplate | stored as is; the golden tag is reported at once so the Agent becomes Ready |
| Actor create | record only, state `SUSPENDED`, no pod |
| Actor resume / first request | one pod named like the Actor, label `compute-type: fargate`, readiness probe from the template wakeup probe |
| `DurableDir` volume | `emptyDir` (lost with the pod) |
| actor identity volume | downward API files from pod annotations (`name`, `atespace`, `uid`) |
| egress trust bundle volume | ConfigMap `ate-trust-bundle` with the system CA bundle |
| suspend | state `SUSPENDED` with a marker snapshot URI; the pod stays for `--suspend-grace`, then it is deleted |
| pause | state `PAUSED`, pod stays |
| delete | pod and record are removed |
| router | h2c reverse proxy keyed by the `ate-target-actor` header, resumes on demand |

Not supported (they return `Unimplemented` or are ignored): snapshots, tags,
checkpoints and forks, worker listing, egress enforcement and credential header
replacement (the policy is stored for API compatibility only), image volumes
(standalone sandboxes), JWT minting.

## Build and deploy

```sh
CGO_ENABLED=0 go build -o fargate-shim ./core/cmd/fargate-shim
CGO_ENABLED=0 go build -o mockllm github.com/kagent-dev/mockllm/cmd/mockllm
# build deploy/Dockerfile with both binaries and /etc/ssl/certs/ca-certificates.crt in the context
```

`deploy/` has the manifests used for the probe: the shim (namespace `ate-system`,
Services `api:443` and `atenet-router:80`), a PostgreSQL for kagent, a mock LLM, a
Harness/Agent, and Helm values for the kagent chart. Needed besides those:

- the `ate.dev` CRDs (`substrate-crds` chart), because the controller reads `WorkerPool`;
- a TLS secret `ate-shim-tls` for `api.ate-system.svc` and its CA in the kagent namespace,
  referenced by `config.substrate.caFile`;
- `kagent-crds` and `kagent` charts, a `kagent-postgres` secret, database roles from
  `go/core/cli/internal/commands/bundled_postgres.sql`.

The runtime image runs as a non-root user and cannot create `/config`; the shim
starts actor containers as root (`--actor-run-as-root`, default true), as Substrate
does inside its sandbox.
