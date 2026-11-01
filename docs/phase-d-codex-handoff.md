# Phase D handoff — local Codex execution boundary

**Starting point:** The design PR (branch `experiment/phase-d-adaptive-continuity-protocol-20261102`) defines the frozen scenario/evidence contract in `docs/phase-d-protocol.md`, `experiments/phase_d_manifest.json`, and `scripts/evaluate_phase_d.py`. Its tests exercise the evaluator using **synthetic fixtures only**. No Phase D empirical result has yet been produced.

**Existing work:** Phase C implementation is in open **PR #5**, commit `04f21de2312a9042a81452b16e74ace2edfe8c36`. The full Phase C results are in open **PR #10** and must remain untouched. The design PR starts from current `main` without taking ownership of those open PRs.

## Execute next, locally

1. Use **separate git worktrees**. Keep `main`, #5, #10, and this design branch unmerged. Base the temporary Phase D implementation worktree on **PR #5 head** if reusing its baseline/fault-provider code; treat the Phase D design branch as a read-only contract. Prefer a *stacked* implementation PR against the PR #5 branch, so unrelated Phase C changes are not disguised as a minimal new diff. Do not force-push #5 or #10.
2. Confirm repo SHA and clean worktrees, Python 3.12+, Docker/PostgreSQL/DBOS availability and test-only source/environment credentials. Run a credential leak scan of environment **names only**. No GitHub PAT, no `gh` writes, no live provider, no external account mutation.
3. Implement a **domain-owned append-only goal/source ledger** distinct from the two arms and from the mock provider. Ensure ledger provenance, independent readback, and test-only executor isolation, including appropriate UID/permissions. The trusted decision source owns v1→v2; only Agent A may update its own Runtime observation, not Agent B. Supersession and A→B authorization are explicit **business-owned records**, not simulated by rewriting an existing operation's owner.
4. Execute the exact causal timeline and four fault cases. Reuse the pre-existing Phase C mock provider/fault scheduling where possible, but record independent evidence that its new `pack/ship/quarantine` contract actually works. Each arm must enforce version checks, grant revocation, immutable operation identity, and deny an outsider.
5. Build a **fair** standalone PostgreSQL+DBOS comparator with the same goal ledger, safeguards, version authority, readback, fault timing, handoff checks, and operator-visible history, **without importing Continuity internals**. Compare reproducible operator reconstruction tasks, not merely application wall time.
6. Run one-seed/8-arm-trial **development** shakedown. Mark artifacts non-acceptance. Repair the harness and record changes before freezing its implementation SHA. Then execute all **24** arm-trials (12 pairs). Abort without manufacturing rows if any fault gate or oracle independence claim fails.
7. Produce `results/phase_d/full-24/trials.jsonl` and `provider_domain_oracle.jsonl` with the **exact precommitted schema**, plus raw provider/goal/source logs, fault traces, environment metadata and cleanup. Evaluate using `scripts/evaluate_phase_d.py` unchanged. Treat exit 0 only as structurally acceptable evidence, **not positive ROI**.
8. Open a small **implementation PR**, and separately a **sanitized evidence/report PR**, both **Open** and unmerged. Include the SHA, runner command, actual exit codes, CI outcome, evidence links, all failed shakedown attempts, null measurement fields and cleanup proof. Never report a simulated `quarantine` as a real shipment operation.

## Stop and report instead of hiding limits

- You cannot enforce an actual source version reader, independent goal ledger ownership, or protected provider oracle.
- Agent B requires taking over Agent A's observation/link owner field to make the demo pass.
- The old operation `ship` executes after trusted v2 evidence, an outsider produces an effect, or a `satisfied` goal is reported with zero confirmed `quarantine` commits.
- The B crash injection does not land after a durable reservation and before POST, or the runner silently retries the uncertain write.
- The design manifest or evaluator would need to change to make observed results green. Escalate the discrepancy; don't rewrite the target.

**Requested response to reviewer:** two PR URLs, exact implementation/report SHAs, full 24-case key counts, evaluator verdict, linked provider and goal ledgers, per-scenario status matrix, operator setup+reconstruction times/costs (null if unknown), exact cleanup, and limitations. Do **not** merge any PR.
