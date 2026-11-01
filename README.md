# Continuity Runtime

[![CI](https://github.com/xiongweilin/continuity-runtime/actions/workflows/ci.yml/badge.svg)](https://github.com/xiongweilin/continuity-runtime/actions/workflows/ci.yml) [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE) [![Python 3.12+](https://img.shields.io/badge/Python-3.12%2B-blue.svg)](pyproject.toml)

**Cross-boundary continuity for agents, workflows and digital systems.** A small, model-agnostic state contract and effect coordination layer, not another Agent framework, workflow engine, personal memory database or global world model.

Status: **engineering alpha / isolated reference integration. Not production-approved.**

## What exists

- **Selective state contracts**: purpose-scoped cross-boundary links, authoritative references, optimistic revisions, source observations and version-dependency invalidation events. No mandatory global resource registration.
- **Effect Coordinator**: immutable operation identity/fingerprint, optional domain-defined logical-effect dedup key, an administrator-issued scoped grant, multiple authorized participants, one durable dispatch reservation, fail-closed handling of uncertain external effects, provider readback, and domain-owned outcome verification.
- **Protocol-level non-substitution**: model/Agent proposal is not authority; an API acknowledgement is not independently verified effect; verified effect is not domain outcome.
- **Bounded enforced admission**: baseline path and authority-owned named admission profiles. A protected canary promotion policy evaluates independently sourced telemetry, initial-stage quality prerequisites, allowed stages, freshness, rollback readiness and performance guardrails. Missing required checker fails closed. This does **not** implement or certify the full BAA protocol.
- **Execution-engine independence**: a DBOS workflow example and an executable two-process handoff using independent Agent identities. Runtime owns neither Agent session state nor DBOS workflow history.
- **Operational tests**: crashes/unknown replies, duplicate dispatch, revocation, stale versions, concurrent PostgreSQL dispatch, API scope and independent process/database handoff.

## Architecture boundary

```text
Agents / Workflows / Controllers
        | intent, observations, necessary relationships
        v
Continuity Runtime (state contracts + effects + admission interface)
        | stable operation identity; scoped grant; dispatch reservation
        v
Trusted, credential-isolated Executor --> external authoritative system
        ^                                         |
        +------------ independent readback --------+
        |
        +------------ domain outcome verifier
```

Database transactions protect **local claims and dispatch rights**, NOT exactly-once effects in arbitrary external systems. In particular, a crash after reserving dispatch causes `may_have_been_sent` until the result is reported or reconciled; never blindly repeat an operation. An external connector must provide its own idempotency, fencing, or reliable reconciliation as appropriate. A fresh admission/authorization check before dispatch does not make a remote state change atomic; external conditional writes are needed when that matters.

**Safety boundary:** for covered effects, all writable external credentials must belong to trusted executors, never the submitting Agents. Without this deployment isolation, a bypass is possible. The API does not expose arbitrary HTTP requests. The bundled sample provider uses a second database and is **only a test system**.

## Start locally

Requires Python 3.12+; real multi-process reliability is intended to use PostgreSQL. SQLite works for isolated demos and unit tests, but its concurrency behavior is not a substitute for PostgreSQL.

```bash
python -m pip install -e '.[dev]'
continuity demo
python examples/canary_guarded_handoff.py
pytest -q
```

Two **separate Python processes** using **two separate SQLite files** (Runtime state vs emulated external authority):

```bash
export CONTINUITY_DATABASE_URL=sqlite:////tmp/continuity-runtime.db
export CONTINUITY_SAMPLE_PROVIDER_URL=sqlite:////tmp/continuity-external.db
python -m continuity.handoff start
python -m continuity.handoff finish
python -m continuity.handoff finish
```

The canary walkthrough performs a **simulated** cross-Agent action: unsafe telemetry denies dispatch, then newly validated mock telemetry allows one operation and separate effect/outcome checks. It uses no real release traffic.

The two-process handoff's first command creates a scoped authority grant, observation, necessary relationship, and operation. The second process executes, reads back, and verifies. The third call proves it will not dispatch a second time. Delete both temporary demo files to restart the example. This is a **local demonstration**, not a live Agent/model integration.

## PostgreSQL + API

```bash
export CONTINUITY_DATABASE_URL='postgresql+psycopg://continuity:password@localhost:5432/continuity'
alembic upgrade head
export CONTINUITY_TOKENS_JSON='{"example-admin-token":{"actor":"root","admin":true},"example-agent-token":{"actor":"agent-a"}}'
uvicorn continuity.api:app --host 127.0.0.1 --port 8080
```

**Replace example tokens before deployment.** There is no default token and no anonymous state access. The sample API deliberately has no configured external effect adapter: attempting to dispatch a provider without an explicitly trusted adapter is rejected. For an emulated integration in a controlled environment, use `continuity.handoff` or register a trusted adapter in the app composition.

The `compose.yaml` profile runs PostgreSQL and the API; set `POSTGRES_PASSWORD` and `CONTINUITY_TOKENS_JSON`. It has no live external effect credentials. Do not expose the API on the public internet without upstream TLS, secret management, network policy and appropriate identity provisioning.

## Integration surface

**Recovery worklist:** `GET /v1/admin/recovery-queue` is a read-only, admin-only view of uncertain effects and verified effects without domain outcome. It cannot cancel, forcibly complete or redispatch operations. There is intentionally no admin "mark success" shortcut.

**HTTP:** `/v1/grants` (admin issue), `/v1/grants/{id}/revoke` (admin), `/v1/observations`, `/v1/links`, `/v1/operations`, `/v1/operations/{id}/dispatch`, `/v1/operations/{id}/reconcile`, `/v1/operations/{id}/verify-outcome`, `/v1/events` (admin).

**Durable handoff checkpoints:** `POST /v1/subscriptions` creates an actor-owned consumer, `GET /v1/subscriptions/{id}` polls from its last acknowledgement and `POST /v1/subscriptions/{id}/ack` advances the revision-checked cursor only up to a previously offered sequence. Delivery is **at least once**, not exactly once; the downstream worker must process events idempotently and acknowledge only after durable work. Two workers using the same consumer can see the same batch.

**Event discovery:** `GET /v1/my-events?after=SEQ` returns `{events, next_seq}` for actor-filtered handoffs. Advance to `next_seq` even when `events` is empty (other actors may have events). It is not a durable multi-consumer queue; consumers must retain their offsets.

**GitHub:** `continuity.github_labels.GitHubLabelAdapter` is an optional scoped, real-HTTP adapter restricted to one repository and an explicit label allowlist. Tests use mock HTTP only; no live repository is mutated. Install `.[github]` to use it. This example is suitable only for the goal that a label is present, not general GitHub writes.

**DBOS:** `examples/dbos_bridge.py` wraps API calls in a durable DBOS step; CI exercises the DBOS workflow against a controlled HTTP adapter and PostgreSQL DBOS history. It requires `.[dbos]` and a configured `DBOS_SYSTEM_DATABASE_URL`; DBOS runs its own execution history, while Continuity stores cross-engine effects.

**Other engines:** any authenticated client can use the same stable operation ID and readback endpoints. The model/Agent is never entrusted with provider credentials or final domain outcome assertions.


## Local / real-environment acceptance

The repository-only CI now starts the complete **Docker Compose** stack:
PostgreSQL schema migration, live HTTP API authentication, failed dispatch
without a trusted executor, event storage, health checks and cleanup.
This uses test-only tokens, not a real provider write credential.

The next gates require an explicitly authorized isolated execution environment.
See [local Codex handoff](docs/local-codex-handoff.md) for the controlled
credential-isolation, real readback and failure-injection protocol. **Do not
provide credentials in chat or commit them to this repository.**


## Phase B: isolated real-provider acceptance

The owner has created a private disposable GitHub test repository and an
unlabeled synthetic Issue #1. **No real label effect has been authorized or
dispatched yet.** The optional `compose.phase-b.yaml` overlay runs a
separate trusted executor with the GitHub credential mounted as a Docker secret,
pinned to that one repository, Issue #1 and label `continuity-effect-test`.
The Runtime sees only an *internal service key*, never the GitHub provider token.
The Agent isolation probe has neither secret nor executor network access.

CI also rehearses the overlay with **fake credentials and no provider writes**:
starts the full stack, checks Agent secret/network exclusion, registers the
scoped operation, and tests logical-effect-key deduplication. This is an
engineering isolation test, not a formal security assessment.

**Offline preflight can run now without any GitHub PAT:**
`python scripts/phase_b_offline.py`. It supplies *synthetic invalid*
credentials, checks the Docker service boundary and stops before `/dispatch`.
A successful result is not evidence of real provider credential isolation.

The local steps, temporary secret-file handling and explicit stop-before-write
point are in [Phase B local handoff](docs/local-codex-handoff.md).

## Guarantees and important non-guarantees

| Mechanism | Current scope |
|---|---|
| Identity rebound protection | Same operation ID cannot silently refer to different canonical intent/participants |
| Dispatch right | PostgreSQL transactional row lock and conditional update permit one durable reservation for a given ID |
| Uncertain network outcome | Conservative `unknown`; same logical operation is never blindly redispatched |
| Readback | Only the trusted adapter can assert an effect; a negative snapshot is not historical no-effect proof |
| Outcome | Separate trusted domain verifier, not an effect receipt; once recorded it cannot be silently overwritten by another verifier |
| Authority | Administrator-owned grants, scoped principal/actor/provider/action/target, optional **mandatory source-version guards** enforced at submit and dispatch |
| Admission | Authority-pinned named checker plus bounded BAA-inspired canary guard using independent telemetry; full BAA risk accounting and policy proof **not included** |
| Global exactly-once effect | **Not guaranteed** without external support |
| Non-bypassable effect | **Requires deployment credential isolation** |
| Production readiness | **Not established**; no real production domain, sustained multi-week run, tenant isolation or independent security audit |

## Architectural admission rule

Only persist a cross-system relationship if it is necessary for **handoff, continuing responsibility, permission, conflict, recovery, or verification**, and cannot be reliably reconstituted from its authority. All domain payloads remain with their owning system.

See [design and risks](docs/architecture.md) and [acceptance matrix](docs/acceptance.md). These documents distinguish existing tests from pending real-environment validation.

## Lineage

Independent successor experiment to [AIOS](https://github.com/xiongweilin/aios) and [BAA-Protocol](https://github.com/xiongweilin/BAA-Protocol). Those repositories remain archived and are **not** dependencies. This project reuses engineering lessons, not old object hierarchies or claims of universal safety.
