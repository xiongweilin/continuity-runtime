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
| Second unrelated real domain | Not yet established | Required before claiming generality |
| Net human/model/resource cost reduction | Not measured | Controlled baseline comparison needed |

## Stop conditions before production

No deployment may claim controlled, non-bypassable writes without evidence that provider credentials are inaccessible to the proposing Agents. No deployment may claim global exactly-once effect without external idempotency/fencing guarantees. No deployment may claim autonomous effectiveness without independently verified target outcomes and accounting of human effort, model costs and incidents.
