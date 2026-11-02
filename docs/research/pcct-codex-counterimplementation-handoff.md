# Local Codex handoff — independent PCCT counter-implementation

**Research gate, not Phase E and not a new Runtime.** Start only when the researcher asks Codex to continue. Source [RFC PR #18](https://github.com/xiongweilin/continuity-runtime/pull/18), source-controlled bounded-cut research [PR #19](https://github.com/xiongweilin/continuity-runtime/pull/19), tracking [Issue #17](https://github.com/xiongweilin/continuity-runtime/issues/17).

## What the independent implementer must try to disprove

The candidate assertion is **not** that cross-agent commitments, handoff signatures or obligation maps are novel; those are prior art. It is the narrower *engineering* hypothesis that a **single portable evidence-oriented profile** can verify complete, explainable accountability transitions across independently owned, heterogeneous workflow data sources with less domain-specific adapter and audit effort than conventional PostgreSQL + application validation + OPA + signed attestations.

To test fairly, treat both alternatives as **equally capable** of enforcing the semantic invariant. Neither may import the other's core checker. The goal is *negative capability discovery* — identify the least complex successful design, even if that makes Continuity unnecessary.

## Local environment and boundaries

- Work in new isolated git worktrees/branches; **do not merge or rewrite** main or PRs #5/#10/#11/#13/#14/#16/#18/#19.
- No GitHub PAT, real Odoo account, production data, external provider writes, or contact with real businesses; use disposable local test keys only.
- No new workflow engine, general Runtime goal API, agent-memory service or persistent cross-domain coordinator. A minimal local PostgreSQL container is acceptable **only for the independent competitor**; report resource cleanup.
- The existing Python model at `research/pcct/semantic_oracle.py` is an *expected-output oracle*, not a sound signed evidence checker. **Do not import or copy its algorithm** into the PostgreSQL competitor. Reimplement from this written contract, with independent SQL schema and authorization rules.

## Tasks in order

1. **Read prior art and state negative results**: IJCAI 2007 commitment machines, IJCAI 2015 observable composite commitment protocols, AAAI 2021 maintenance commitments, RFC 9162 and in-toto bundle limitations. Confirm no novelty claim is being made.
2. **Document the trust model**: what exactly source owners authenticate; who attests that all in-scope open obligations and prior effects are included; how the cut identity/revision is tied to a query and policy; when “incomplete” must be returned. Distinguish detecting altered signed data from proving an owner didn't omit a never-recorded event. The latter is impossible absent owner governance/trust.
3. **Independent reference solution**: PostgreSQL `open_obligations`, `external_effects`, `source_revisions`, `grants`, `transitions`, `dispositions` with appropriate transaction isolation/constraints; an explicit domain validation procedure and optionally OPA. Attestations may use the same trusted issuer fixture as the PCCT approach. **Do not penalize the baseline for not inventing requirements PCCT itself lacks.**
4. **Heterogeneous source fixtures**: at least two *separately authored* synthetic upstream formats: (a) append-only domain events (goal, obligation, effect), (b) workflow execution history or DBOS/PostgreSQL record stream. Neither depends on the current Continuity Runtime's internal table layout. Build adaptations under the same source cut/completeness assumptions. A third independent format is welcome only if inexpensive.
5. **Cross-check adversarial traces**, including: clean complete cut; omitted second obligation; omitted committed effect; duplicate successor; unsigned/unsupported obligation release; stale source cut; unknown previous side effect; absent issuer completeness; altered signed digest; equivocal/competing source cuts; former agent self-granting replacement authority; old grant used after revocation; valid independent operation unaffected by v2.
6. **Quantify incremental value** rather than just tests: number of source-specific adapters, changed lines when adding a new source/obligation type, reviewer trace reconstruction steps, total policy/schema/checker code, setup/maintenance effort and trust dependencies. Record elapsed/operator seconds if *actually timed*, else null. Never equate “passed all tests” with novelty.
7. Produce an independent comparison report with **explicitly attempted counterexamples**. If the PostgreSQL/policy competitor passes all cases with equal or less adaptation/review cost, mark **NO DIFFERENTIATOR**. If a gap appears, isolate its exact property and check whether a generic attestation profile instead of a new Runtime removes it.

## Output structure

- A **separate draft implementation/counterexample PR**, preferably stacked on PR #19 without modifying its frozen oracle, with independent SQL/policy, ephemeral-key proof fixtures and test harness.
- A **separate results-only PR** (open/unmerged) containing case-by-case results, raw test outputs, environment/hash metadata, measured integration/reviewer effort (or null), and a conclusion `NO_DIFFERENTIATOR`, `PORTABILITY_CANDIDATE` or `INCONCLUSIVE`.
- No repo merge. No edits to past Phase C/D evaluation or historical artifacts.

## Immediate stop conditions

- Source-issuer completeness cannot be established under the declared threat model — return `INCOMPLETE_SCOPE`, document impossible claim, do not fake trust.
- Validating the same certificate requires controlling unrelated workflow/business state — report architectural premise failure.
- A trusted cut is used to authorize a later unguarded provider write — fail; the proof does not eliminate TOCTOU.
- The challenger is intentionally weakened to “a few SQL scripts”, or copies the original research checker implementation — reject the comparison as biased.
- A reviewer asserts guaranteed originality, globally complete obligations, or better ROI from synthetic cases — stop the claim.

**The immediate success criterion for Codex is a credible independent attempt to reject PCCT, not a working new Continuity product.**
