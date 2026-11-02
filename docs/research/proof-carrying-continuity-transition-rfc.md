# Research RFC — Proof-Carrying Continuity Transition (PCCT)

**Date:** 2026-11-02  
**Status:** **unproven mechanism hypothesis, not an implemented feature, product claim, or proposal to restart the generic Runtime**. No real provider, account or customer operations are authorized. The engineering freeze in [the Phase C/D decision record (draft PR #16)](https://github.com/xiongweilin/continuity-runtime/pull/16) remains applicable; that decision record is currently a separate *draft* PR and need not be merged to conduct paper-level research.

## 0. Why restart *mechanism research*, not Runtime development?

The old Runtime put durable operations, scoped grants, readback and verification around DBOS/PostgreSQL; Phase C produced the same recovery outcomes as a strong baseline. In Phase D, the most important semantics — goal supersession and accountable A→B handoff — were implemented by a **domain-owned ledger in both arms**, with equal reconstruction steps. This is direct evidence **against** the differentiating mechanism of the previous design, not evidence against adaptive continuity as a research question.

A credible alternative must have a precise property not supplied merely by another retry engine, workflow history table or permission check. We will reject new code-first feature development until the property and a fair competing construction are specified.

## 1. Prior-art challenge: what is *not* new?

| Mechanism people might call “continuity” | Established prior art | Implication |
|---|---|---|
| Durable execution, restart and step/transaction replay | [DBOS Workflow guarantees](https://docs.dbos.dev/typescript/tutorials/workflow-tutorial) and [Temporal workflow versioning](https://docs.temporal.io/develop/go/workflows/versioning) | Do **not** build a rival workflow engine. |
| Pausing, changing and migrating a running process definition | [Camunda 8 process-instance migration](https://docs.camunda.io/docs/components/concepts/process-instance-migration/), [Temporal Signals/Updates](https://docs.temporal.io/encyclopedia/workflow-message-passing) | “Old goals can change” is not a unique product capability. |
| Revision-dependent beliefs, explanations and retraction | Jon Doyle's [Truth Maintenance System (1979)](https://dspace.mit.edu/entities/publication/5377b306-4ecc-4687-b1f5-78cbb4a0543a) | Evidence invalidation and dependency-directed recomputation are prior art. |
| Causal provenance, agents and responsibility/delegation | [W3C PROV-O](https://www.w3.org/TR/prov-o/) | Agent–activity–entity attribution is an existing vocabulary. |
| Versioned policy decisions and audit | [OPA decision logs](https://www.openpolicyagent.org/docs/management-decision-logs) | Authorization history is not unique. |
| Provenance and portable signed claims | [in-toto attestations](https://github.com/in-toto/attestation), [OpenLineage](https://openlineage.io/docs/spec/facets/job-facets/) | A signed handoff JSON blob or dependency graph alone is not novel. |
| Bilateral agent action attestation | [Internet-Draft: bilateral cross-organization agent action](https://datatracker.ietf.org/doc/html/draft-mih-agent-bilateral-attestation-02), September 2026 **draft, not a final standard** | Requiring two signatures is not sufficient differentiation either. |
| Independent assurance profiles composed by action digest | [Internet-Draft: Agent Accountability Composition](https://datatracker.ietf.org/doc/draft-mih-sato-agent-accountability-composition/), August 2026 **individual draft, not a formal standard** | Combining CAN/WHO/WHAT/AUDIT proofs for an agent action is already proposed. PCCT must address **bounded open-obligation accounting across changing goals**, not merely combine signatures or provenance. |

**Finding:** there is no demonstrated new *primitive* in the existing Runtime. The research candidate below is a proposed **compositional verification property**, not an assertion of being the first ever to combine these concepts. A more exhaustive literature/patent search would still be needed before any originality claim.

## 2. Candidate: Proof-Carrying Continuity Transition (PCCT)

**One-sentence hypothesis:** a small, engine-independent verifier could accept or reject a **change of direction** only when the relevant prior effects, revisions, outstanding responsibilities, and new authority are accounted for by independently sourced, linked evidence — without needing to replay or own either workflow's internal state.

This is a **transition contract and verifier**, *not* a task scheduler, agent memory store, global graph, universal authority or replacement for domain truth.

### 2.1 Inputs and ownership

Let a domain-owned transition request refer to four independently sourced, selectively scoped sets:

- `F`: historical fact/revision attestations, with explicit provenance, source authority and validity horizon;
- `G`: goal/decision history and supersession basis, including who is authorized to declare a goal no longer binding;
- `X`: externally confirmed effect history and ambiguous effects **as separate dispositions**, verified against the respective provider where needed;
- `O`: **open responsibility/obligation register**, including origin, scope, current accountable authority, evidence dependencies, and permitted dispositions.

`O` is the candidate's extra semantic commitment. It cannot be inferred safely from a generic event stream: a business/domain owner must declare which responsibilities exist and which are discharged or superseded. Each source owner signs or otherwise securely authenticates its own attestations; the verifier does **not** become the source of truth.

A transition proposal `T` carries references/digests to a snapshot or freshness token for each selected source, **an independently owned index/cut of all in-scope previously committed effects**, affected-obligation mapping, transition decision, and replacement owner(s) with explicit acceptance. **The source owners must attest that their indexes are complete for the declared bounded scope**; otherwise omission cannot be distinguished from absence and the correct result is `INCOMPLETE_SCOPE`. Missing or stale evidence produces `HOLD`, **not** a fabricated conclusion.

### 2.2 Core conservation rule (accountability conservation, not obligation immortality)

For every in-scope **open** obligation at the accepted source cut, exactly one **explicitly authorized accounting disposition** is required:

1. `retain`: a named accountable owner retains it (and its conditional authority is reevaluated);
2. `fulfill`: authoritative completion/outcome evidence discharges it;
3. `supersede`: a competent domain decision terminates the old obligation, recording reason, basis, and **who remains accountable for any residual consequence**;
4. `transfer`: former authority releases/records disposition and new authority explicitly accepts within its permitted scope; on partial or failed acknowledgment the obligation remains `unresolved` with an identified accountable custodian;
5. `hold`: obligation remains open, but future dependent writes are prohibited until uncertainty is resolved by the relevant owner.

Splits/merges are allowed **only** with a checked, provenance-linked mapping covering all inputs and all successor obligations; the rule does not mean an obligation can never legitimately disappear. No owner can silently erase an obligation by declaring a process completed. A lost/disconnected agent does not automatically give its successor write authority. This rule is meaningful only where the domain owner has a sufficiently complete register; otherwise the verifier must state `INCOMPLETE_SCOPE`.

### 2.2a Concrete gap in Phase D (not a vulnerability finding)

Source review of Phase D [implementation PR #13](https://github.com/xiongweilin/continuity-runtime/pull/13) shows explicit domain-owned `handoff_authorized` goal events and a B-only new action grant. It does **not** expose an explicit, independently enumerated *open-obligation register with exhaustive transition dispositions* in the reviewed domain-owner and worker modules. Phase D therefore demonstrated safe handoff of the **scripted scenario**, not the stronger proposition that no unmentioned responsibilities could be orphaned.

**Distinguishing negative trace:** At v1 A has both `verify-PO07` and `notify-supplier` outstanding. The v2 handoff transfers `verify-PO07` to B, validly revokes A's action grants, and records goal supersession, but **omits `notify-supplier` entirely**. A normal signed handoff and workflow audit can be internally consistent. A scoped PCCT obligation cut containing both entries would reject/hold the transition until the second responsibility is explicitly accounted for.

**Strongest rebuttal:** a PostgreSQL `open_obligations` table with a transactionally enforced full mapping, versioned owner approvals, plus existing signing/OPA tools can reject exactly this trace. PCCT's only possible *incremental* claim is **portable verification across independent authority-owned ledgers without a shared transaction or bespoke in-process state model**; that claim still requires proof. This is not a claim of a security defect in Phase D's declared acceptance criteria.

**Proof obligation C1:** no unaccounted prior open obligation within the explicitly declared scope.  
**Proof obligation C2:** no conflicting active owner/authorization for the same protected action/effect key without domain-sanctioned concurrency rules.  
**Proof obligation C3:** prior effect and prior evidentiary claims are carried forward *as history*, even if their justification for future actions has expired.  
**Proof obligation C4:** newly admitted future actions have fresh source-version guards and proper owner-approved authority, with provider-enforced conditional writes where TOCTOU matters.  
**Proof obligation C5:** only the dependency-reachable action frontier is held; unaffected actions remain eligible, subject to their own independent guards. This is a precision goal, not guaranteed when dependency declarations are incomplete.

**Example:** a v1-approved purchase request caused Odoo draft PO-07 (effect confirmed). At v2 a supplier restriction invalidates the purchase goal. A has an open task `check_supplier`; an old `dispatch_order` must be denied. If B accepts a new `investigate_supplier` obligation under fresh authority, the verifier should approve **that handoff** while preserving PO-07 as historical effect. If A's unresolved `verify_PO07` obligation is missing from the mapping, the certificate must be rejected as **orphaned responsibility**, even if all workflow histories and signatures themselves are valid.

### 2.3 Candidate portable transition certificate

```json
{
  "protocol": "pcct-hypothesis-v0",
  "transition_id": "transition-002",
  "scope": {"case_id": "purchase-7", "open_obligation_cut": "source-owned-cut-44"},
  "supersedes_goal": {"old": "purchase-v1", "new": "investigate-v2",
                       "decision_ref": "goal-owner:decision-92"},
  "basis": [{"source": "supplier-authority", "old_revision": "v1",
             "new_revision": "v2", "evidence_ref": "supplier-event-18"}],
  "effect_index_cut": "odoo:owner-scoped-cut-17",
  "historical_effects": [{"effect_key": "odoo-draft-07",
                          "disposition": "confirmed",
                          "independent_readback_ref": "odoo:readback-217"}],
  "obligations": [
    {"id": "verify-po-07", "disposition": "transfer",
     "from": "agent-a", "to": "agent-b",
     "release_ref": "old-owner:release-9",
     "accept_ref": "new-owner:accept-13",
     "authority_ref": "procurement:grant-v2-6"},
    {"id": "dispatch-order", "disposition": "supersede",
     "decision_ref": "goal-owner:cancel-10",
     "residual_custodian": "procurement"}
  ],
  "independent_attestation_refs": ["goal-owner-log", "source-event-log",
                                    "provider-effect-ledger"],
  "preconditions": {"authority_revision": "2", "source_revision": "v2"}
}
```

This is **illustrative data, not a secure token or accepted wire standard**. The syntax, cryptographic binding, expiry and schema are intentionally not frozen. Domain events, provider receipts and authorization must be authenticated at their actual owners. A hash alone does not make an assertion true.

### 2.4 Required result types

The same proof checker should return structured explanations, never silent repair:

- `VALID`: all in-scope conservation and freshness checks pass; this does **not** certify that an external side effect will succeed.
- `HOLD`: source changed, evidence is stale, an effect is ambiguous, or another party's acceptance is missing. Still require an accountable custodian.
- `INVALID`: an old effect is rewritten, a responsibility is orphaned or double-assigned, a grant is unauthorized, or the transition contradicts an independent attestation.
- `INCOMPLETE_SCOPE`: missing or unverifiable source ledger/cut/open obligations; no completeness claim.

The verifier can check a transition's **justification relative to an explicitly bounded source cut**. It cannot guarantee a globally atomic snapshot across organizations, prevention of bypassing provider controls, completeness of undisclosed obligations, or future business-goal satisfaction.

## 3. Why this could differ — and how it could fail

**Potential architectural distinction:** DBOS/Temporal maintain *execution* continuity within their workflow histories. A cross-owner PCCT verifier would check the **legitimacy and accounting completeness of a transition in objective, evidence and responsible party**, including external effects, using owner-issued witnesses from multiple independent workflow engines. It would only need the selected evidence slice, not full workflow engine replay or custody of every domain object.

This distinction may collapse under a fair alternative: a small PostgreSQL obligation table + OPA policy + signed PROV/in-toto attestations + a domain-specific validator could satisfy C1–C5 with equal portability and less complexity. If so, **PCCT is merely a useful pattern/format, not a reason for another independent runtime**.

Do **not** move the goalposts by comparing to a naive retry script. The competing implementation gets identical owner-supplied obligation registers, attestations, policy revisions and protected provider facts. Evaluate *incremental contract expressiveness, portability, proof explainability and implementation/maintenance work*, not whether one implementation is named Continuity.

## 4. Fastest falsification before any local Codex or Docker

Perform **paper + pure-function trace validation** only, no new orchestration engine. First publish the obligations and expected outcomes for these adversarial traces:

| Trace | Expected |
|---|---|
| clean v1→v2, verified old effect, complete accepted transfer | `VALID` |
| old effect omitted from transition | `INVALID` |
| one open obligation has no successor/disposition | `INVALID` |
| two active successor owners for a singleton protected effect | `INVALID` |
| new Agent claims old grant without fresh scoped authority | `INVALID` |
| unsigned or not-yet-acknowledged transfer | `HOLD` |
| source revision changes between attested cut and dispatch | `HOLD` (provider still enforces guarded writes) |
| source ledger is incomplete or not independently trusted | `INCOMPLETE_SCOPE` |
| unrelated operation not dependent on revised evidence | remains eligible; avoid globally blocking |
| old effect confirmed but new goal cancelled | retain the effect; deny stale future action |
| previously dispatched effect has unknown disposition | `HOLD`, no blind retry or erasure |

**Discriminating construction:** one independent checker receives equivalent evidence traces from (a) DBOS/PostgreSQL and (b) a different workflow representation or purely domain-owned ledger. Its verdict should not depend on runtime-specific database tables. A separately authored **best-practice PostgreSQL + policy** checker must attempt the same traces without importing PCCT code. Give the competing approach credit for all capabilities it actually has.

### Predeclared rejection criteria

Reject PCCT **as an independent product/runtime candidate** if any holds:

1. The C1–C5 guarantee is indistinguishable from an ordinary per-application obligation transition validator with documented PostgreSQL constraints and policies.
2. Correct verification secretly requires the verifier to own workflow/ERP state or to pretend source and obligation completeness that independent owners never asserted.
3. Cross-engine evidence interoperability requires so much custom adapter logic that the portable contract saves no maintenance or review effort.
4. The claimed safety property disappears with expected concurrent source updates, disconnected stakeholders, bypassable provider writes, or ambiguous prior side effects.
5. The mechanism only produces prettier logs and cannot identify a concrete responsibility gap a fair comparator misses.

**Advance only the mechanism hypothesis** if a minimal portable certificate exhibits a strict, testable combination of C1–C5 and a meaningful interoperability/review advantage against that strongest comparator. Even then the appropriate implementation is initially a **standalone verifier/specification**, not an execution Runtime. Business ROI research comes *after* technical differentiation, never as a substitute for it.

## 5. Explicit non-actions

- Do **not** merge Phase C/D PRs or overwrite their negative/inconclusive comparative results.
- Do **not** contact external customers or access real providers.
- Do **not** start Phase E, add lifecycle state machines or build a new persistence layer.
- Do **not** infer that a certificate is sound because its JSON parses, its hash chain links or one mocked trace passes.
- Keep [pilot admission Issue #15](https://github.com/xiongweilin/continuity-runtime/issues/15) conditional; no customer pilot until a distinguishing mechanism and owner-authorized evidence exist.

**Research decision:** the earlier idea `continuity = durable execution + grants + verification` has no established differentiator. `continuity = externally checkable conservation of responsibility across justified change` is a **specific, falsifiable replacement hypothesis**. Its novelty, technical feasibility, and independent-architecture value remain unproven.
