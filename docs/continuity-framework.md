# Continuity: conceptual framework and evolution

**Status:** long-term design framework, not a claim of full implementation or a universal data ontology. The Runtime selectively coordinates state across Agents, workflows, and independent domains; domain facts, business policy and outcome authority remain with their owners.

## 1. Four state concerns — what needs continuity?

These are **conceptual concerns**, not four mandatory database tables or a requirement to persist everything.

| Concern | What must survive change | What must remain changeable |
|---|---|---|
| **Facts and evidence** | What was observed or happened, source, time/revision and provenance | Current interpretation, confidence and source validity; an observation is not automatically authoritative truth |
| **Goals and judgments** | Why a goal or decision was adopted, its basis and prior decisions | Goal validity, priorities and conclusions when trusted new evidence arrives |
| **Execution and results** | Operation identity, dispatch uncertainty, receipts, independently confirmed effects and outcomes | Responsible Agent/workflow, recovery strategy and future work; an external effect is not undone by changing a goal |
| **Authority and accountability** | Who authorized, acted, verified, revoked or handed off, with an auditable sequence | Current permissions, accountable owner and whether work should continue |

Continuity therefore **preserves the historical record without making any past objective, judgment, executor or authorization permanently binding**. Preserve only purpose-necessary, provenance-aware references; do not build a global world model.

## 2. Cross-boundary state contracts — how are relationships constrained?

A cross-boundary contract is an explicit, selectively maintained relationship between independently owned states. It should specify **purpose, owner, participants, scope, source/basis reference, authoritative evidence or version where relevant, revision/invalidation conditions, and responsibility for verification**. It must also distinguish identity and authority: observing a fact does not grant permission to produce an effect.

**Current engineering mechanisms:** purpose-scoped links, optimistic revisions, source observations with provenance, dependent-link invalidation, fixed operation identities, scope-limited revocable grants, independently read source versions, durable effect reservation, reconciliation and domain-owned outcome verification.

**Important boundary:** local source-version checks are not atomic with a remote provider mutation; external conditional writes or provider-side guarantees may be necessary. A receipt does not prove an effect, and a confirmed effect does not prove the business goal.

## 3. Three analysis layers — what constitutes success?

Use the same separation as [architecture.md](architecture.md):

1. **State expression:** what relevant facts, relations, intents and revisions can be represented, with provenance and limited scope.
2. **Continuity and constrained consistency:** how authority, identity, version checks, handoff, effect fencing, uncertain-state recovery and audit survive boundary changes.
3. **Overall effectiveness:** whether independently verified business outcomes, adaptability and total human/model/operational costs justify the coordination layer compared with simpler alternatives.

Passing layer 1 or 2 does **not** imply layer 3. The Runtime coordinates selected boundaries; models, Agents, workflows and domain verifiers remain external owners of much of the end-to-end result.

## 4. Dynamic feedback and evolution — how should reality change the system?

When trusted external evidence conflicts with a prior assumption, **append the new evidence and re-evaluate** the affected interpretation, dependency, admission and goal. Do not silently rewrite what was previously observed or claimed. A revision may invalidate a link or hold future dispatch; it does not retroactively erase a completed provider effect or automatically authorize a compensating write.

A desired lifecycle eventually supports **pause, terminate, supersede and restart**, plus explicit actor/owner replacement with responsibility transfer, while retaining the history of prior authorizations, decisions, verified effects and unresolved uncertainties. A replacement objective or execution attempt needs clear identity and authority boundaries; never silently recycle a prior operation identity or retry an ambiguous effect.

### Present capabilities versus open design work

| Capability | Status |
|---|---|
| Selective relation/observation records, revisions and source-dependent invalidation | Implemented in the engineering alpha, subject to stated trust limits |
| Revocable scoped grants, multiple authorized participants, stable operation identity and durable dispatch reservation | Implemented in the engineering alpha |
| Receipt/readback/independent domain outcome separation, fail-closed uncertainty | Implemented in the reference effect path; provider-specific guarantees differ |
| General lifecycle semantics for pausing, terminating, superseding or restarting goals/processes | **Long-term goal; not a complete implemented lifecycle protocol** |
| Versioned revision of judgments and explicit transfer of accountability across arbitrary domains | **Long-term goal; requires domain-owned authority, evidence and policy rules** |
| Proven usefulness and cost advantage across unrelated real domains | **Not yet established** |

**Design test:** after a goal, belief, owner or executor changes, can the system still explain *what was known, who was responsible, which effects actually occurred, and what remains possible or authorized next*—without compelling the old plan to continue? That is the intended meaning of continuity.
