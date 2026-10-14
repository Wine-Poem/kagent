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
| Actor create | record only, state `SUSPENDED`; with `--eager-start` its pod starts right away |
| Actor resume / first request | one pod, label `compute-type: fargate`, readiness probe from the template wakeup probe; a warm pod is claimed when the pool has one |
| `DurableDir` volume | `emptyDir`, saved to and restored from the snapshot store (see below) |
| actor identity volume | downward API files from pod annotations (`name`, `atespace`, `uid`) |
| egress trust bundle volume | ConfigMap `ate-trust-bundle` with the system CA bundle |
| suspend | `/data` is saved first; state `SUSPENDED` with a marker snapshot URI; the pod stays for `--suspend-grace`, then it is deleted |
| pause | state `PAUSED`, pod stays |
| delete | pod and record are removed |
| router | h2c reverse proxy keyed by the `ate-target-actor` header, resumes on demand |

Not supported (they return `Unimplemented` or are ignored): process-memory snapshots, tags,
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

## Faster starts and persistent `/data`

- `--eager-start` (default on) starts an Actor's pod when the Actor is created, so the
  microVM boots while the user is still typing.
- `--warm-pool=N` keeps N idle pods per runtime image. Their main container runs this
  binary as a wrapper (`wrapper` subcommand) that waits for an activation, restores `/data`,
  writes the identity files, sets the environment of the template and replaces itself with
  the runtime. A claimed pod serves one Actor and is never reused; the pool refills itself.
  Pools are keyed by everything that shapes the pod except its env, so agents that share a
  runtime image share a pool. Unused pools are dropped after `--warm-idle-ttl`.
- `--snapshot-url` turns on `/data` persistence. A small store (`store` subcommand,
  `deploy/snapshot-store.yaml`) keeps one gzip tar per Actor **UID**. Every pod gets a
  `snapshotter` sidecar; the shim calls it on suspend, and a restore init container (cold
  pods) or the wrapper (warm pods) puts the data back. The token of a pod is
  `HMAC(secret, actor uid)`, so it opens only that Actor's object; deleting needs an admin
  token that only the shim has. A new Actor never inherits data, because the key is the UID
  and deleting an Actor deletes its snapshot.

Create the shared secret before deploying: `kubectl -n ate-system create secret generic
ate-snapshot-secret --from-literal=secret="$(openssl rand -hex 24)"`.

Only `/data` is kept. Process memory and anything written outside `/data` are lost when a
pod goes away, and data written after the last suspend is lost if the pod dies before the
next one.

## What works

Verified on EKS Fargate with `probe` and the kagent CLI:

- Sessions: create, A2A request and streaming, delete, idle expiration, many sessions at once.
- Human in the loop: an `ask_user` pause keeps the pod; the answer resumes the paused operation,
  also when the pod was lost in the meantime (Pause saves `/data` first).
- Checkpoints and forks: a checkpoint keeps a copy of the saved `/data`, a fork starts a new Actor
  from it. The snapshot scope is DATA when the snapshot store is on, which is what kagent needs to
  allow forks.
- Standalone sandboxes: create, exec, file upload and download, suspend and resume (the workspace
  is restored), delete. The guest image is unpacked into an `emptyDir` by an init container.
- Tools over MCP and shared sub-agents (they run inside the session's pod).
- Recovery: a lost pod shows up as SUSPENDED and the next request starts a new pod that restores
  `/data`; controller and shim restarts do not interrupt sessions.

## What does not work

- Credential injection: the runtime keeps the placeholder API key `kagent-credential-injected`.
  Use a provider that needs no key from the pod (Bedrock with IRSA), or a model server inside the
  cluster.
- Egress allow-lists: the policy is stored but not enforced.
- Process-memory snapshots: only `/data` survives a pod loss.
- Substrate sandbox classes (gVisor, microVM), workers, JWT minting, access policies.

A resume call waits `--resume-wait` for the pod and then reports the real state. kagent gives the
call 30 seconds and a cold Fargate pod needs about 45, so creating a sandbox from a template without
a ready warm pod fails once with "retry with request-id" and the retry succeeds. Reporting RUNNING
early is worse: the first request then misses its own deadline, and kagent warns that a command
"may have started". With a ready warm pod the sandbox is usable at once.

`probe` (`go run ./core/cmd/fargate-shim/probe -agent NAME stream|hitl|fork|isolation`) and the
`deploy/probe-*.yaml` fixtures reproduce the tests. `echollm` is a model that reports how many user
turns it received and whether a marker word is in the history, which shows what a restored or
forked session still remembers.
