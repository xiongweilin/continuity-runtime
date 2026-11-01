# Phase C — Failure recovery versus a fair PostgreSQL + DBOS baseline

**Status:** experimental protocol and evaluator implemented. **NO paired runtime benchmark has yet been executed.** Only synthetic evaluator unit tests have run.

**Safety:** Local disposable PostgreSQL + DBOS and a separately persisted mock provider. No GitHub PAT, live Issue labels, production environment or other real provider effects. A fake provider's success is not evidence of production safety. The completed Phase B token should be revoked by its owner.

## Falsifiable question

Does Continuity reduce **verified recovery effort, human intervention and total cost** enough to justify an additional coordination layer, compared with a well-engineered PostgreSQL + DBOS + scripts implementation?

Arm A, **continuity**: use the real Continuity grant/version preconditions, durable effect reservation, trusted executor adapter, reconciliation and independent domain verifier. Use PostgreSQL + DBOS for workflow history, and count the API/executor overhead.

Arm B, **postgres_dbos**: implement an independent PostgreSQL unique logical-effect key, transactional reservation, equivalent scoped grant/version preconditions, DBOS history, provider readback, unknown-state policy and domain verifier. No Continuity imports/tables. **Never deliberately weaken the baseline to make Continuity look better.**

Both arms use the same independent, separate-process mock provider, instrumented fault scheduler, independent provider ledger, per-trial workload, input, delays, admission rule and state reset. The provider records every POST and every applied mutation; it must NOT automatically collapse duplicate posts. The oracle is exported by the provider, not synthesized from either arm's database or its own reported receipt.

## Frozen matrix (full run = 8 cases × 10 seeds × 2 arms = 160 trials)

Source of truth: experiments/phase_c_manifest.json. A quick development subset is NOT the full run.

| Scenario | Deterministic injection | Expected provider commits | Allowed reported state |
|---|---|---:|---|
| clean | No fault | 1 | satisfied |
| ack_lost_after_commit | Apply commit, lose HTTP acknowledgement before local receipt | 1 | satisfied via fresh readback |
| crash_after_reservation_before_send | Kill sender after durable reservation, before POST | 0 | unknown |
| timeout_before_send | Timeout before any POST; negative snapshot cannot prove historical no-effect | 0 | unknown |
| concurrent_duplicate_intent | Independent actors, distinct operation IDs, same logical effect key | 1 | satisfied |
| revoke_before_dispatch | Revoke grant before reservation | 0 | blocked |
| stale_version_before_dispatch | Authoritative state version changes before reservation | 0 | blocked |
| readback_unavailable_after_commit | Provider commits; block all authoritative readbacks | 1 | unverified |

Do not blindly retry ambiguous operations. Under a missing authoritative readback, the provider's hidden truth can be known to the experiment oracle but must **not** be passed into the tested system as an oracle. Otherwise the experiment cheats. For revocation and stale versions, the baseline must implement the equivalent block rather than simply ignore the restriction.

For each seed, deterministically shuffle which arm runs first; record shuffle algorithm and order. Pin all versions, OS, timestamps and test resource limits. Reset PostgreSQL and DBOS workflow state between trials, fresh provider ledger per trial. Instrument fault barriers to prove the exact injection point was reached (reservation committed before crash; provider committed before ACK discarded; concurrent submitters actually overlapped). If barrier is missed, trial is invalid, not silently successful.

## Required independent evidence

Two JSONL artifacts, one record per scenario + seed + arm (**160 each**):

**results/trials.jsonl**, from the arm instrumentation, with exactly:

- scenario: string; seed: integer 0–9; arm: continuity or postgres_dbos.
- reported_status: satisfied, blocked, unknown or unverified.
- outcome_evidence: true only if independent goal-verifier reference exists.
- elapsed_ms: wall-clock time in a matching time window.
- provider_calls, db_transactions, recovery_actions, manual_interventions: nonnegative integers.
- operator_seconds, model_cost_usd, infra_cost_usd: measured nonnegative numbers, or null when unknown; **never silently substitute 0 for unknown**.

**results/provider_oracle.jsonl**, exported by the separately durable provider ledger:

- scenario, seed and arm as above.
- commits, duplicate_commits, forbidden_commits, provider_post_count: nonnegative integer counts.
- effect_present: boolean; oracle_log_ref: nonempty ledger reference independently reviewable.

Reports must also include **results/run_metadata.json**: code SHA for both arms, PostgreSQL, Python, DBOS and mock-provider versions, local OS/host class, DB schema version, isolation configuration, seed and run ordering, precise fault barrier evidence, time/cost measurement sources, real exit codes and cleanup results. Do not upload raw secrets or sensitive host paths.

Run the evaluator only on **real produced trial records**, not a fabricated example:

~~~bash
python scripts/evaluate_phase_c.py \
  --manifest experiments/phase_c_manifest.json \
  --results results/trials.jsonl \
  --oracle results/provider_oracle.jsonl \
  --output results/evaluation.json
~~~

Exit 0 means a **structurally valid and not automatically unsafe** dataset, **not** positive ROI. Exit 2 rejects missing/unpaired/invalid evidence. Exit 3 indicates an observed safety failure. The evaluator compares every arm/seed/case, checks provider truth and counts, flags duplicated/forbidden commits, false declarations of satisfaction, certainty claims under ambiguous no-effect, and unresolved recovery. It does not verify the independent ledger's cryptographic provenance or decide business value; these require review.

If a case cannot meet its predefined fault barrier or there are missing rows, **abort** rather than fill synthetic successes. Keep raw results and failure details in a separate sanitized PR. Run first a **two-seed shakedown**, clearly labeled development-only. Freeze code before all 160 production-of-evidence trials. The Phase C runner and baseline implementation are **still work to be done by local Codex**, not implied by these evaluator files.

## Independent decision rules

**Safety gate:** zero forbidden/duplicate provider effects, false satisfied outcomes, ambiguous no-effect claims or mismatched independent truth. Any such failure halts further benchmarking and triggers root-cause analysis.

**Recoverability:** compare satisfied, blocked, unknown/unverified outcomes and interventions per fault type. Both arms must handle the identical case and event schedule. If a successful commit has no authoritative readback, do not claim a verified business result.

**Business value target (predeclared decision heuristic, not a statistical theorem):** compared with baseline, Continuity must reduce measured human interventions by at least **25%** OR measured operator time in recoverable fault cases by at least **20%**, while p95 wall time stays under **2×** baseline and attributable measured cost per verified outcome increases less than **30%**. Compute absolute differences, per-scenario changes and uncertainty. Include setup, maintenance and developer effort separately; do not infer those from LOC alone. Where cost data is null, decision = **INCONCLUSIVE**, not “no cost”. If safety is comparable but no meaningful gain emerges, prefer the simpler PostgreSQL+DBOS solution or narrow Continuity to the contexts where it shows an advantage.

Do not claim generality: simulated one-domain trials do not establish production non-bypassability, multi-domain value, exactly-once external effects or a full BAA implementation.

## Execution ownership

- **ChatGPT:** experimental contract, evaluator/unit tests, review of fault-barrier logs and results, recommendation to continue/narrow/stop. Do not self-certify the runner's claims.
- **Local Codex:** implement the independent mock-provider oracle and both real PostgreSQL+DBOS arms in an isolated experiment branch; run two-seed smoke, then full 160-trial experiment; record resource costs, preserve sanitized evidence; clean Docker resources. No real GitHub provider writes.
- **Human owner:** approves any departure from the local simulated-only scope; previous one-off sandbox write authorization is consumed.

Keep implementation PR and experiment-data/report PR separate. Never merge existing PRs #2/#3/#4 as an automatic side effect of Phase C. Stop and report blockers if a test requires external credentials, live provider writes or altering the frozen matrix to obtain a favorable score.
