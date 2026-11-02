# Decision record — research freeze and real-pilot gate (2026-11-02)

**Status:** proposed documentation-only freeze, **not** a deployment or a code change.  
**Decision:** **KEEP-NARROW** — stop proactive expansion of the generic Continuity Runtime. Preserve existing evidence and components; only reopen implementation against an independently verified, costed, owner-authorized business problem.

## Evidence basis and limits

| Evidence | Confirmed in that bounded setting | Not established |
|---|---|---|
| [Phase B live GitHub effect acceptance (PR #4)](https://github.com/xiongweilin/continuity-runtime/pull/4) (open PR #4) | One authorized sandbox label operation was independently read back and confirmed | General production security, exactly-once delivery, usefulness or ROI |
| [Phase C](https://github.com/xiongweilin/continuity-runtime/issues/6) (open PRs #5 and #10) | 160 simulated arm-trials; both arms had equal goal/recovery counts, zero recorded safety violations, no measured human-intervention advantage | Infrastructure cost, setup/maintenance burden, incremental business benefit |
| [Phase D](https://github.com/xiongweilin/continuity-runtime/issues/12) (open PRs #11, #13 and #14) | 24 simulated arm-trials; both arms had 9/12 satisfied and 3/12 correctly unresolved, 0 recorded safety/functionality violations; historic partial effects, goal changes and handoffs tracked | Advantage in operator reconstruction (60 steps per arm), measured operator time or ROI, database-level isolation of Agent identities |

In Phase D, Continuity's median elapsed time was 2,920 ms versus 3,055 ms for the PostgreSQL+DBOS arm, but the sample was only 12 pairs with similar p95 latency. This is not evidence for a product advantage, especially with **unknown human/infrastructure cost**. In both experiments the strong baseline passed the same safety outcomes. The long-term [adaptive continuity framework](../continuity-framework.md) remains a design thesis, not a product-market-fit claim.

**Do not merge the five experimental PRs simply to mark the study "finished."** They form a reproducible evidence set and should be retained open/unmerged until the owner explicitly chooses how to archive or integrate them. This document is a decision record, not an instruction to close those PRs.

## Real-use-case discovery, rather than another synthetic phase

Searched existing project GitHub repositories and project-specific Google Drive/email references for an operator-owned case. **No verified paying user, accountable external process owner, real deployment, manual work baseline or realized savings was found in the accessible evidence.** That is an evidence gap, not a proof of no possible demand.

**Highest-priority *technical* candidate: [PurchaseFlow](https://github.com/xiongweilin/PurchaseFlow).** Its local demo already integrates an actual Odoo 19 Community instance with role-separated approvals, frozen input revisions, PostgreSQL outbox, idempotent ERP intents, authoritative readback, UNKNOWN reconciliation and mismatch detection. It is more realistic than repeating a GitHub label simulation, yet still uses **synthetic procurement data and demo identities**; it is **not yet a qualified customer pilot**. Do not duplicate the mechanisms PurchaseFlow already has in a new coordinating service.

**Business hypothesis to test — not a reported incident:** after a procurement approval and a persisted Odoo draft, a trusted later supplier, stock or price change invalidates the original goal. Another permitted procurement owner must reconstruct the prior approval, preserved Odoo effect, version change and responsibility transfer; the superseded goal must not continue. Measure the current PurchaseFlow workflow *before* layering in Continuity.

**Secondary technical candidate: [ReturnOps-Mini](https://github.com/xiongweilin/ReturnOps-Mini).** It covers customer service/warehouse/finance handoff and uncertain refunds but uses a simulated payment provider and synthetic cases. Do not start an independent comparison until there is a genuine operator-owned use case.

## Admission and measurement contract

The operative project gate is [real-pilot qualification Issue #15](https://github.com/xiongweilin/continuity-runtime/issues/15). Its checkboxes are **not yet satisfied**. Require:

1. **Process owner and consent:** a real authorized business owner confirms a recurring change/handoff problem and explicitly authorizes discovery. Do not name private persons or publish confidential examples.
2. **Observed examples:** capture redacted real case IDs, at least five if available, and the role/system/evidence revision, ownership change and real cost of each. Do not invent case frequency.
3. **Current-system baseline:** time and steps to reconstruct a changed purchase request; correction and reconciliation frequency; number of handoffs; time to closure; cost of failures; developer setup and maintenance effort. Record unknown values as unknown.
4. **Authorization boundary:** the domain owns goal validity and responsibility transfer, Odoo owns external order facts, and no Agent receives unrestricted ERP writes. Use read-only shadow observation and least-privilege data first.
5. **Fair comparator:** existing PurchaseFlow/PostgreSQL/outbox/Odoo versus a narrowly integrated Continuity path, **same** actor boundaries, version guards, audit, reconciliation and workloads. Predeclare metrics and a meaningful improvement threshold relative to integration and maintenance cost.

**GO** only after the gate is satisfied and the pilot is explicitly authorized. Begin with non-effecting observation. Any live ERP mutation or cost-bearing external action requires a **separate**, precisely scoped approval. **NO-GO** for more generic Runtime development absent these conditions.

## Ownership and next action

- **Assistant:** complete the archive/decision record; review new independently sourced customer/problem evidence when it exists; design only a narrow, falsifiable pilot if approved. This does not authorize contacting third parties.
- **Codex:** **no task now**. Do not modify, merge or deploy the Runtime; do not build Phase E, generic goal-lifecycle APIs or a new synthetic comparison.
- **User/business owner:** supply real use-case authority or evidence when available. Until then, the defensible outcome is to **stop engineering work** while preserving the research.

**Repository boundaries on 2026-11-02:** `main` stayed at `24332318334c932c2bd7d6133419d806f28801e0` during this research-freeze preparation; the Phase C/D implementation and result PRs remain open and unmerged. This record is on its own documentation branch for review.
