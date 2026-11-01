# Phase D — Adaptive continuity across changed evidence, goals and executors

**Decision context:** Phase C produced equal simulated safety/recovery outcomes for Continuity and a competent PostgreSQL+DBOS baseline, with no measured advantage and unknown infrastructure cost. Phase D is a **small, falsifiable test of the project's longer-term thesis**, not a reason to restart broad Runtime development.

**Status:** protocol only. No Phase D workload has been run, no new lifecycle feature is implemented, and no real provider may be called. Freeze source: [phase_d_manifest.json](../experiments/phase_d_manifest.json), 4 cases × 3 seeds × 2 arms = **24 arm-trials / 12 pairs**. Development shakedown is excluded from acceptance.

## Central question

After an authoritative decision changes from v1 to v2, can the system preserve (a) historic facts and earlier external results, (b) the reason an old goal was superseded, and (c) who was permitted to decide and act, **without** continuing the obsolete plan or silently granting the replacement Agent old privileges?

A positive demonstration of this behavior **does not automatically prove the Continuity layer is necessary**. Compare against an independently implemented, equally competent PostgreSQL+DBOS solution.

## Verified architecture fit before implementation

Existing `src/continuity/runtime.py` on main supports: purpose-scoped links and revisions, actor-owned observations with prior revision events, dependent-link invalidation on source-revision change, independent authoritative version checks at dispatch, immutable operation identity, revocable grants, fixed authorized participants, effects/reconciliation, separate outcome verification, and an actor-filtered event feed.

**Not implemented as general Runtime primitives:** an explicit goal lifecycle with pause/terminate/supersede/restart; a versioned belief/conclusion revision protocol; or a transactionally enforced ownership/accountability transfer from one Agent to another. Notably, one actor **cannot rewrite another actor's observation/link**, and adding a new actor to an existing operation is not a general authority-transfer operation. The `effect_confirmed` status concerns an external effect, **not** the enduring validity of its business objective.

**Design constraint:** for Phase D, place goal lifecycle and authorized handoff in a minimal **domain-owned, append-only ledger**. Keep its owner/authority outside Runtime and outside each tested arm's self-reported status. Do not implement a generic goal engine, mutate an old operation's immutable identity, impersonate the original observation owner, or claim native Runtime support for these domain transitions.

## Minimal mock domain — shipment review

A simulated shipment case has a source-owned risk decision, a mock external provider with independently logged `pack`, `ship`, and `quarantine` effects, a business-goal ledger, and two separate Agent identities.

The initial business goal is `prepare_shipment`; the later goal is `quarantine_and_review`. Packing is a **verified partial external effect**, not proof that the original shipment goal is satisfied. Once risk evidence changes, old packing **remains a historical fact** and may not be erased, compensated or silently repeated.

### Required causal timeline (both arms)

1. Trusted decision source emits risk **v1** = approved to pack. Business owner appends `goal_created(old_goal, basis=v1)`; Agent A records a provenance-bearing v1 observation and a purpose-scoped link dependent on v1.
2. Owner issues distinct scoped `pack` and `ship` grants for **Agent A only**, with required authoritative version v1. A registers both operations with distinct identities/effect keys. A packs once via credential-isolated mock provider, reconciles independent provider truth and verifies the **pack operation**. The registered `ship` operation must **not** be dispatched yet.
3. Decision source emits trusted **v2** = fraud hold. The **original observation owner A** records a versioned v2 observation referencing independent evidence; the v1-dependent link becomes inactive and emits an invalidation event. In a controlled test order, A attempts to dispatch the existing v1 `ship` operation **before** grant revocation: it must be held for stale version, with **zero shipment POSTs**.
4. The business owner appends `goal_superseded(old_goal, new_goal, basis=v2)`, revokes A's old write grants, then explicitly appends `handoff_authorized(new_goal, Agent B, authority-ref)`. The owner issues a **new, separate** B-only `quarantine` grant guarded on v2. Agent B obtains a domain-authorized projection of prior verified work; it is **not** allowed to assume it can read A-only private Runtime operation/event streams.
5. B registers a **new** operation and effects `quarantine` once, or deliberately remains unresolved under the injected crash. The independent mock provider confirms what did and did not happen. The domain verifier may report new-goal satisfaction only after independently observing quarantine **and** no shipment. Domain goal satisfaction is never inferred from B's submission or provider receipt.
6. Archive append-only goal/source/provider event histories, original grants and revocations, operation identities, source v1/v2 evidence, active/inactive link revision, actor-authorization decisions and verification references. Preserve old pack despite goal supersession. Do not rewrite old operation outcome.

For lost ACK, the mock provider must commit `pack` and drop the acknowledgement before the arm consumes it; no blind retry, independent readback recovers prior presence. For intruder attempt, an unauthorized third actor tries to access/dispatch without authority and gets denied; no secret or effect exposure. For B's crash, kill **after** durable quarantine reservation **before** any provider POST, preserve uncertainty and do not self-award goal success or blind-retry. Retain negative snapshots as insufficient proof of historical absence.

## Four frozen scenarios

| ID | Injected change | Provider truth | New business-goal status |
|---|---|---|---|
| `normal_supersession` | Normal v2 replacement and A→B handoff | pack 1, ship 0, quarantine 1 | satisfied |
| `pack_ack_lost_then_supersession` | Lost first pack ACK after durable provider commit | pack 1, ship 0, quarantine 1 | satisfied |
| `unauthorized_actor_during_handoff` | Unauthorized outsider attempts protected operations | pack 1, ship 0, quarantine 1 | satisfied |
| `new_action_crash_before_send` | Kill B after durable reservation but before quarantine POST | pack 1, ship 0, quarantine 0 | **unresolved** (correctly conservative) |

No case gets an unearned green status: a correctly unresolved case is a *safety pass*, not a successful business outcome.

## Fair controls and evidence

**Arm A (continuity):** current real Continuity API/runtime for operations, grants, versions, links, effects, readback and verification. Use the external domain-owned goal ledger to represent lifecycle transitions not in Runtime.

**Arm B (postgres_dbos):** an independent PostgreSQL+DBOS implementation with **equivalent** effect fencing, authorization/revocation, source-version guards, source/goal history, immutable operation IDs, outbox/audit, handoff restrictions, reconciliation and domain goal verification. Reuse the independent baseline from open PR #5 only as a starting point (in a separate worktree/stacked PR). Do **not** import Continuity internals or weaken the baseline.

Both arms must use the **same** independently persistent mock provider, same domain goal owner, same fault points, same ordered decisions, same data and same fixed seed; alternate arm run order per seed. Never mix old Phase C trial rows with Phase D evidence. No GitHub PAT, live GitHub issue, production provider, or non-test credential.

The provider ledger must be exported from the provider, not reconstructed from a Runtime operation row; the goal ledger must come from the domain owner, not from either arm's description of what happened. Preserve raw tamper-evident append-only logs and check their hash chains; a hash chain alone cannot prove who generated a record. Test-only file/process permissions must keep providers and oracle logs out of Agent access. Independent readback is mandatory to justify both pack confirmation and new-goal satisfaction.

**Per trial, require:** exact scenario/seed/arm, commit/post counts for each action, duplicate and forbidden counts, A's stale-dispatch denial proof, link-invalidation event ref, retained v1/v2 source references, old-goal supersession and B handoff authorization with actor and decision ref, pack readback ref, provider/goal ledger refs, phase-specific fault barrier proof, B's operation generation, business-goal outcome and verifier ref (or explicitly none), process exit codes, measured time/DB work, human interventions, model/infra cost or `null`, host/version/cleanup metadata. Do not mark unsupported claims successful or treat null cost as zero.

### Acceptance and decision

1. **Hard safety gate:** no ship after v2, no unauthorized/duplicate effects, no stale old operation dispatch, no reclassification of historical pack, no rewritten old goal/observation record, no unapproved handoff, no false outcome after crash. Every barrier must be genuinely reached. Any violation halts positive acceptance.
2. **Functionality gate:** old evidence and pack remain reconstructable; v2 invalidates dependent link; A's stale operation stays blocked; old A write grants are revoked; B acts only under new permission; an unresolved new effect remains visibly unresolved without automatic retry.
3. **Incremental value gate:** compare **manual reconstruction steps, operator time, setup/maintenance effort, extra state coordination, and attributable costs**. If both arms satisfy the same trace with no measured improvement, the verdict is **no demonstrated incremental value**, even if both pass safety. Missing cost => ROI **INCONCLUSIVE**, not success. With only 12 pairs, latency and cost differences are descriptive, not general statistical proof.
4. **Decision rule:** prefer PostgreSQL+DBOS for this scenario unless Continuity shows a materially useful, reproducible advantage in cross-boundary evidence reconstruction or responsibility handoff at acceptable additional cost. **Do not build a generic goal-lifecycle subsystem from this study.** If no advantage, retain the design thesis and narrow existing effect coordination.

## Execution plan and stop boundary

- **ChatGPT now:** inspect current implementation, freeze this manifest and acceptance protocol, create an open design PR plus a tracking issue; provide a precise Codex handoff. These steps do not claim any Phase D experiment ran.
- **Local Codex next:** start from an isolated, fresh worktree; implement only the mock domain ledger and runner necessary for the frozen four scenarios. First run a single-seed development shakedown (8 arm-trials; not part of acceptance). Fix issues in a separate implementation branch, freeze the source, then run 24 arm-trials. Preserve independent oracle and source ledger. Separate code and evidence PRs, with exact CI and exit codes.
- **ChatGPT after evidence exists:** independently cross-check output against the frozen manifest, inspect fault barriers and raw logs, and decide KEEP-NARROW versus a justified incremental capability. Do not merge any previous experiment PR as an unintended dependency.

A blocked transition or unresolved outcome is **honest continuity** when the system retains the necessary facts, responsibility and safe next options; it is not a reason to manufacture success.
