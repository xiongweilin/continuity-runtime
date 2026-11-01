# Acceptance and evidence matrix

Tests in this repository are not a production guarantee. CI runs isolated SQLite tests and a real PostgreSQL service.

| Requirement | Test or demonstration | Remaining uncertainty |
|---|---|---|
| Cross-agent identity without shared session | `test_process_handoff.py`, `continuity.handoff` | Real LLM Agent integrations |
| Independent external data ownership | Sample connector uses a distinct database | Real provider readback coverage |
| No same-ID duplicate dispatch | `test_dispatch_competition_no_duplicate_effect` and PostgreSQL logical effect key race | Domain-defined effect_key cannot identify unknown semantic duplicates |
| PostgreSQL concurrent dispatch | `test_concurrent_dispatch_across_independent_runtime_instances` | Network partitions, worker kill faults |
| Crash after effect before response | `test_crash_after_effect_before_ack_retains_unknown` | Full provider-specific reconciliation |
| No-effect not inferred from missing record | `test_negative_snapshot_does_not_prove_no_effect` | Historical no-effect proofs |
| Authority revoked before dispatch | `test_authority_rechecked_at_dispatch` | Real-time upstream IAM revocation |
| State dependency changed | `test_source_version_changes_block_dispatch` | Authoritative remote version check at commit |
| Handoff relationship invalidation event | `test_link_revision_invalidation_and_owner` | Multi-consumer processing and backpressure |
| Bounded BAA-inspired admission | `test_canary_admission.py`, `test_canary_walkthrough.py`: trusted stage telemetry, denied/held/admitted, authority-pinned profile, independent version check, actual simulated effect | Full BAA, aggregate risk budgets, non-bypassable deployment and causal safety/cost study remain unproved |
| Mediated HTTP surface | `test_mediation_surface.py`: frozen route inventory, authentication dependency checks, effect-route delegates | Cannot prove no raw Python, leaked credential or network bypass |
| Contradictory domain verifier concurrency | `test_concurrent_verifiers_cannot_overwrite_first_committed_outcome` | Later business-state changes require separately versioned domain handling |
| Grant revocation event propagation | `test_revoked_grant_event_reaches_affected_agents_not_strangers` | Eventual delivery/consumer operation in real deployment |
| Outcome is not receipt | `test_no_auto_outcome_and_domain_verifier` | Independent real-world outcome measurement |
| Cross-workflow engine interoperability | Same HTTP interface; DBOS workflow runs against isolated HTTP test adapter using its own durable Postgres state | Live DBOS vs a second workflow engine in different hosts |
| Durable event consumption | Actor-owned DB checkpoint, CAS ack, bounded poll | Distributed consumer leases and production backpressure |
| Full container deployment | Compose startup, Alembic migrations, authenticated HTTP and fail-closed dispatch checked by `scripts/smoke_compose.py` | Real external credentials and network isolation not checked |
| Second unrelated real domain | Not yet established | Required before claiming generality |
| Net human/model/resource cost reduction | Not measured | Controlled baseline comparison needed |

## Local Phase A acceptance — reported 2026-11-01

**Reporter:** local Codex run as supplied by the operator; this entry records the reported execution, not independent access to the workstation.

- **Source baseline:** `e4c90db4a7d49156b4f40a5a4a4de2a611ade1ae`, with clean checkout matching `origin/main` at the time of the run. This baseline independently passed [GitHub CI](https://github.com/xiongweilin/continuity-runtime/actions/runs/41267817646) prior to local acceptance.
- **Runtime:** Python 3.12.13; editable install `.[dev,dbos,github]` in local `.venv`.
- **Tests:** full `pytest -q` reported **77 passed, 0 skipped, 0 failed**, including actual PostgreSQL 17 concurrency and DBOS history tests with `CONTINUITY_TEST_POSTGRES_URL` set. One Starlette/httpx deprecation warning remained.
- **Isolated flows:** bounded canary demonstration and Docker Compose smoke reported passing; authenticated API, blocked missing executor, and persistent event behavior were observed in the test environment.
- **Cleanup:** the locally created PostgreSQL container and volume, Compose containers/networks/volumes and test ports were reported removed. `.venv` and local image cache retained. A previously policy-blocked temporary Docker CLI configuration directory was intentionally left untouched.
- **Limits:** no provider write credentials, real external system mutation, production data or Phase B tests. Local success does **not** establish external credential isolation or independent provider-side reconciliation.

**Decision:** Phase A's local engineering checks are complete based on the operator's report. **Phase B remains blocked for real writes** until the owner chooses an explicit disposable repository, issue/label scope, credential permissions and an isolated trusted-executor boundary.

## Stop conditions before production

No deployment may claim controlled, non-bypassable writes without evidence that provider credentials are inaccessible to the proposing Agents. No deployment may claim global exactly-once effect without external idempotency/fencing guarantees. No deployment may claim autonomous effectiveness without independently verified target outcomes and accounting of human effort, model costs and incidents.
