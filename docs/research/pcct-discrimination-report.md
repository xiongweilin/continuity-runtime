# PCCT mechanism discrimination — first formal counterexample and next gate

**Status: early research, 2026-11-02.** This report is **not** a claim of invention, of sound cryptographic verification, or of business ROI. The source-controlled `research/pcct/semantic_oracle.py` is a tiny pure-function *expected-result oracle* whose inputs have already been resolved by **independent trusted owners**. It has no DB connectivity, provider executor, signature verification, Merkle proofs, or live authorization. Synthetic tests do not prove a secure protocol.

## Result so far: much stronger prior art than anticipated

1. Commitment lifecycle and delegation are established. [Yolum/Singh-style formal communication protocols](https://ijcai.org/proceedings/2007/245) already discuss create, cancel, release, assign, delegate and discharge.
2. Crucially, [Baldoni, Baroglio, Chopra and Singh, IJCAI 2015](https://ijcai.org/proceedings/2015/9) already studied **compositional verification of social commitments under partial observability**. They show that composing individually verifiable protocols can produce a protocol that is **not** verifiable, and propose additional social-context bridging commitments. This sharply weakens any claim that PCCT invented cross-owner responsibility verification.
3. [Maintenance of Social Commitments (AAAI 2021)](https://ojs.aaai.org/index.php/AAAI/article/view/17355) already relates goal/commitment coherence and maintaining desired conditions.
4. An [October 2026 research preprint on Responsibility-Centered Coordination](https://arxiv.org/abs/2610.21243) explicitly proposes responsibility-preserving delegation and recovery chains, though it is a preprint and its reported experiments are not independently validated here.
5. [RFC 9162](https://www.rfc-editor.org/rfc/rfc9162.html) specifies append-only log inclusion and consistency proofs; **these do not prove the issuer recorded every relevant real-world obligation**. The [in-toto Bundle specification](https://github.com/in-toto/attestation/blob/main/spec/v1/bundle.md) explicitly notes that a bundle is not authenticated as a whole and omissions/replays need mitigation. Authenticated database **query completeness** is itself a researched problem ([survey](https://link.springer.com/article/10.1007/s41019-016-0018-9)).
6. [Camunda process-instance migration](https://docs.camunda.io/docs/components/concepts/process-instance-migration/) already demands mappings for active elements in an individual process instance. [Temporal](https://docs.temporal.io/encyclopedia/workflow-message-passing) and [DBOS](https://docs.dbos.dev/explanations/migrating-from-temporal) already offer runtime-level durable event/workflow control. PCCT cannot claim that checking a transition mapping is new.

**Finding:** neither obligation mapping, cross-party responsibility, dependency invalidation, source provenance, nor signed evidence independently differentiates Continuity. Even their broad combination has significant prior art. The only remaining *possible* niche is a **simple portable interoperability/profile contract** for checking already-authenticated bounded cross-owner transitions over *heterogeneous source event formats*, without adopting a common workflow runtime. This is an interoperability/assurance hypothesis, not a new foundational algorithm.

## Two impossibility / non-guarantee arguments

### A. No sound negative conclusion without completeness

Let `view(T)` be all evidence accessible to a deterministic verifier. Consider two worlds: `W1` has open obligations {A}; `W2` has {A,B}, but B is invisible to the verifier. If `view(W1) == view(W2)`, any checker necessarily produces the same verdict in both. A checker that certifies complete accounting of {A} in W1 must therefore **falsely certify** W2 unless it requires a trusted owner asserting the **scope and completeness** of the obligation cut (or a stronger verifiable query-completeness proof). Exactly the same indistinguishability applies to an omitted committed external effect.

Consequently a source owner's `scope-cut + completeness assertion`, appropriately authenticated and checked, is **a trust assumption**, not a theorem produced by a JSON or a hash. No independent owner => **INCOMPLETE_SCOPE**, never `VALID`. Hidden facts held by an uncooperative owner remain unknowable, even if every recorded hash chain is valid.

### B. No global live safety from a read-only transition certificate

Let a verifier check source revision v2 and grant B at time t. If another owner changes its source to v3 at t+1, a prior `VALID` result cannot prevent a subsequent nonconditional provider write at t+2. The result proves only a **bounded attested decision at a cut**, not that the latest world still agrees. Live execution requires independently enforced version-fenced writes, short-lived acceptance, or equivalent source/provider control. This is a TOCTOU constraint, not solvable by signing the certificate.

The proposal is **not** a replacement for hard business authorization, provider-side idempotency, durable effect fencing or domain-owned outcome adjudication.

## Frozen semantic challenge (not security conformance)

`research/pcct/semantic_oracle.py` defines a read-only oracle over two **separate** inputs:

- `resolved`: a *trusted-resolver supplied* bounded cut and complete enumerations of open obligations and external effects, source revision, and issued grants. Test-only `cuts: true` flags **stand in for** a genuine trusted resolver; the submitter must not be able to set these in a real verifier.
- `proposed`: desired new goal, dispositions for every open obligation, acknowledgment and replacement authority, preserved effect references, and future-version-guarded action intentions.

Four result states: `VALID` (only relative to the resolved cut), `HOLD` (freshness, unknown effect or incomplete handshake), `INVALID` (known contradictory accounting/authorization), and `INCOMPLETE_SCOPE` (inadequate trusted source coverage).

Semantics tested independently from workflow engines:

| Adversarial trace | Expected |
|---|---|
| Source-owner completeness cuts, two accounted duties, one confirmed effect, properly accepted transfer | VALID |
| Second duty omitted from otherwise signed-looking handoff | INVALID |
| Confirmed old effect omitted from the new history | INVALID |
| Effect disposition unknown despite a log of the attempt | HOLD |
| Trusted obligation/effect cut missing | INCOMPLETE_SCOPE |
| Transfer not accepted by B | HOLD |
| B granted wrong domain scope or wrong operation | INVALID |
| A's responsibility fraudulently attributed to a different old owner | INVALID |
| Source rev advanced from attested v2 to v3 | HOLD |
| One v1-dependent future action stale, while an unrelated v1 grant has no dependency on revised source | HOLD globally; unrelated action *eligible in isolation* |
| Provider does not enforce required version fence | HOLD |
| Duplicate claimed obligation or historic effect | INVALID |
| Goal owner decision evidence missing | HOLD |
| Supersession lacks residual accountability | INVALID |
| Replacement grant revoked | INVALID |
| Unrelated outsider attempts a future action | INVALID |

Tests describe **known-good/bad fixtures**, not new empirical successes. Coverage of trust resolution, source signatures, non-equivocation, Merkle proofs, split/merge obligations, provider conditional writes, dynamic concurrent acceptance and real cross-engine adapters is deliberately **not claimed**.

### Why a pure checker cannot establish differentiation

A PostgreSQL-backed owner-managed obligations table can represent the same bounded snapshot. A policy engine such as OPA can implement exactly the same predicate; signatures and PROV/in-toto predicates can transmit the cut. **If both consume identical, equally trustworthy inputs, the oracle's four-valued function is straightforwardly implementable in either architecture**. No new computability class, consistency guarantee, or uncopyable safety property is established by the PCCT representation itself.

A hypothetical *new* result must concern **fewer independent custom adapters, easier auditing of third-party evidence, or robust interoperability with no shared state authority**. Those are measurable engineering claims and are not yet demonstrated.

## Predeclared independent comparison

**Next task is an adversarial *counter-implementation*, not expanding Continuity Runtime.** Assign an independent implementer (local Codex) to reproduce the same judgments in **PostgreSQL + application policy/OPA + owner-issued attestation contracts**, without importing `research/pcct/semantic_oracle.py` (only its written I/O contract, expected traces and source trust assumptions). It must get **all** the same owner cuts, schemas and mutation cases as PCCT.

- **Correctness:** both checkers must flag the missing duty/effect, authority mismatch, uncertain effect, non-atomic freshness and the independent unaffected action; no strawman baselines.
- **Portability:** author adapters from **at least two different source representations** (e.g., append-only domain event log and DBOS/PostgreSQL event records). An adapter is a *static, synthetic owner-authenticated witness fixture* until real authority/provenance is possible. Record how much domain-specific code is needed for *each* approach.
- **Completeness:** explicitly account for which source owner asserts bounded completeness; model a missing or equivocal issuer and return `INCOMPLETE_SCOPE`. Do not self-certify absent work.
- **Maintainability:** record implementation/test lines, separate policy/schema configuration, source-owner adapter count, custom query complexity, time spent reading/reviewing a handoff, external assumptions and unresolved limitations.
- **Security claims:** no live provider call or write; no unaudited “signature” boolean as proof. Real auth/evidence validation is outside the current scope.
- **Stopping rule:** if the mature-stack comparator obtains the same correctness and interoperability without material extra domain-specific integration cost, **reject PCCT as an independent Runtime/product mechanism**. A shared specification or domain profile may still be useful. If a real, defensible portability gap survives, limit the next artifact to a **standalone verification interface**, not an executor or orchestration framework.

## Boundary before handing off

The assistant has performed prior-art triage, narrowed the research question, constructed falsifiable trace semantics, and can validate its own model using CI. A *genuinely independent* mature-system counter-implementation is valuable precisely because assistant-authored competing code may inherit the same assumptions and errors; that independent implementation is the next appropriate **local Codex** step.

Neither existing [Phase C/D experiments](https://github.com/xiongweilin/continuity-runtime/issues/12) nor [research PR #18](https://github.com/xiongweilin/continuity-runtime/pull/18) should be merged as a side effect. Research is a stage of **mechanism discrimination**, not a revival of the previous general Runtime.
