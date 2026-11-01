# Phase C full-run report (simulated provider only)

**Status:** Full 160-trial run completed; evaluator exit 0; verdict `EVIDENCE_INCOMPLETE` because attributable infrastructure cost is unknown. This is not a production-safety or positive-ROI claim.

## Provenance and environment

- Implementation PR: [#5](https://github.com/xiongweilin/continuity-runtime/pull/5); code commit [`04f21de2312a9042a81452b16e74ace2edfe8c36`](https://github.com/xiongweilin/continuity-runtime/commit/04f21de2312a9042a81452b16e74ace2edfe8c36).
- Frozen run ID: `full-20261101T132522Z-218b56`; runner window `2026-11-01 13:25:22.152008–13:34:40.580638 UTC` (host Compose wrapper completed at `13:34:43.4717386 UTC`).
- Matrix: 8 frozen scenarios × 10 seeds × 2 arms = 160 arm-trials / 80 paired trials; seeds `0–9`; no evaluator run on development rows.
- Python `3.12.16`; DBOS `2.31.1`; SQLAlchemy `2.1.6`; psycopg `3.3.6`; PostgreSQL `17.11`.
- Python base: `python:3.12-slim@sha256:aa943ce411423c7582288f18707ff66eddcbaeb510502d747a72aeb34f30d159`; PostgreSQL image digest: `sha256:b8faeff7712663c2773a190181e1246e37e364b887611c04658c28b44e2408fe`; runner image ID is recorded in `run_metadata.json` and `host_run_metadata.json`.
- Host: Windows 11 Pro `10.0.26300`, AMD64; Docker Engine `29.9.2`, Compose `5.6.0`; Linux/amd64 containers. Resource limits: runner 2 CPUs/4 GiB, PostgreSQL 2 CPUs/2 GiB; provider-client timeout 0.7 s; arm-worker timeout 180 s.
- Protocol SHA-256: `0afb7bc54c9062d8b15dde62ba9a35b09f289267db0011355566f069cd5d9ff8`; manifest SHA-256: `c5b71b025ca4ced53e8b76c9aa137f75829f2cdc7ee959539ad66d7cae1ca448`; evaluator SHA-256: `8fa4613f1589d48a9b6cb2b0b526d5b707110ea47573af94382b291ed50d77d1`; dependency-lock SHA-256: `3cb6e1da15c4584cee9f864cc5a9cfcd090c5dbcba09b79e361af8e93525e87f`.
- Network/isolation: Compose internal network with no published ports; no-egress policy; provider and fault proxy bound to loopback. A separate preflight confirmed direct unauthenticated provider GET/POST returned 401, arm UID could not read either ledger or provider/proxy process environments, and the probe caused zero provider commits. No GitHub credential or production-provider credential was passed; no external provider or GitHub write was used during trials.

## Integrity and safety results

- The runner produced 160 `trials.jsonl` rows, 160 provider-oracle rows, 160 trial-evidence rows, 160 provider ledgers, 160 proxy logs, and 160 per-trial diagnostics. Pair keys were unique and one-to-one; the frozen evaluator was run only on these full-run data.
- Evaluator exit code: `0`; `safety_violations=[]`; `recovery_status_mismatches=[]`; verdict: **`EVIDENCE_INCOMPLETE`**.
- Independent provider oracle totals: **80 commits**, **80 POSTs received**, **0 duplicate commits**, **0 forbidden commits**; 40 commits per arm. Provider truth matched the injected effect expectation for every trial.
- Each arm reported 30 `satisfied`, 20 `blocked`, 20 `unknown`, and 10 `unverified` outcomes. All 80 paired outcomes agreed and matched the frozen expected status:

| Scenario | Reported state per arm (10 trials) | Provider commits per arm |
|---|---|---:|
| `clean` | 10 satisfied | 10 |
| `ack_lost_after_commit` | 10 satisfied | 10 |
| `crash_after_reservation_before_send` | 10 unknown | 0 |
| `timeout_before_send` | 10 unknown | 0 |
| `concurrent_duplicate_intent` | 10 satisfied | 10 |
| `revoke_before_dispatch` | 10 blocked | 0 |
| `stale_version_before_dispatch` | 10 blocked | 0 |
| `readback_unavailable_after_commit` | 10 unverified | 10 |

No `blocked` claim followed a provider commit; ambiguous no-effect cases remained `unknown`; unavailable readback after commit remained `unverified`. Satisfied claims were cross-checked to provider-owned readback evidence with the expected resource value.

## Paired operational metrics

Differences are **Continuity minus PostgreSQL+DBOS**. Bootstrap intervals are descriptive percentile 95% intervals from 20,000 paired resamples; per-scenario `n=10`, overall `n=80`. They are not certification or broad-population guarantees. Full values and reproducible summaries are in `paired_analysis.json` and `evaluation.json`.

| Metric | Continuity | PostgreSQL+DBOS | Paired difference / observation |
|---|---:|---:|---|
| Trials | 80 | 80 | 80 matched pairs |
| Satisfied / blocked / unknown-or-unverified | 30 / 20 / 30 | 30 / 20 / 30 | 0 paired status disagreements |
| Provider commits / calls | 40 / 40 | 40 / 40 | 0 difference |
| Recovery actions | 100 | 100 | 0 difference |
| Manual interventions | 0 | 0 | 0 difference |
| Operator seconds | 0.0 | 0.0 | 0 measured difference in timed trials |
| Model cost | $0.00 | $0.00 | No model calls |
| Infrastructure cost | null | null | Not attributable from this local Docker run |
| Total database transactions | 27,247 | 27,125 | +122 overall (+1.525 per pair; paired-bootstrap 95% CI +0.10 to +2.975) |
| Median elapsed time | 1,641.773 ms | 1,605.304 ms | +36.423 ms mean paired delta (95% CI +20.192 to +56.033 ms) |
| P95 elapsed time | 4,489.537 ms | 4,460.184 ms | Ratio 1.0066×; under the predeclared 2× ceiling |

Transaction counts include runner-side barrier/status reads and controller mutations in the same measurement window; they should not be interpreted as isolated application-only query counts. Per-scenario latency and transaction deltas are in `paired_analysis.json`.

## Decision under the predeclared heuristic

- **Safety:** passed for this mock-provider matrix; both arms behaved identically on the frozen outcomes.
- **Recovery/effort:** no observed status, provider-call, recovery-action, intervention, or operator-time advantage for Continuity. Both arms required zero human intervention/time in this timed simulation; this does not establish zero setup or maintenance effort.
- **Latency:** P95 stayed below 2× baseline, but Continuity was modestly slower on the median and paired mean in this environment.
- **Cost:** infrastructure cost is null, so attributable cost per verified outcome and the `<30%` cost-increase condition cannot be assessed. The evaluator correctly returns `EVIDENCE_INCOMPLETE`; missing cost is not treated as zero.
- **Conclusion:** **INCONCLUSIVE on business value.** The run does not demonstrate a recovery or operator-effort gain that would justify the added layer for this specific simulated workload; the simpler PostgreSQL+DBOS baseline is sufficient on the measured outcomes. Do not infer production ROI, non-bypassability, exactly-once external effects, or performance outside this local environment. Setup/maintenance/developer effort was not measured, and LOC is not used as a proxy.

## Aborted development attempts and cleanup

A prior development-only attempt using the earlier concurrency barrier aborted when one `concurrent_duplicate_intent` submit interval failed the overlap check. It preserved 19 completed arm-trial rows plus the failed-trial diagnostic under `../aborted-development/concurrent-submit-overlap-miss/`; the evaluator was not run and no rows were filled in. The harness was changed to a shared submit gate plus a symmetric 100 ms test-only hold, then passed the final 32-row development shakedown and this full run. A prior database-URL setup attempt also aborted before any worker started; its run metadata and diagnostic are under `../aborted-development/role-password-url-error/`.

For the full run, 160/160 trial databases were dropped, barrier files were removed, Compose cleanup exited `0`, and post-cleanup container/volume/network counts were all `0`. Raw result artifacts were preserved outside the checkout before Compose volumes were removed.

## Artifact index

- `trials.jsonl`: arm instrumentation rows.
- `provider_oracle.jsonl`: separately exported mock-provider ledger rows.
- `provider_ledgers/`, `fault_proxy_logs/`: raw independent hash-chained ledgers.
- `trial_evidence.jsonl`, `trial_diagnostics/`: barrier proofs, per-trial process exits, cleanup, and evidence references.
- `run_metadata.json`, `host_run_metadata.json`: versions, hashes, seed/order, isolation, resource limits, exit codes, and cleanup.
- `evaluation.json`: frozen evaluator output; `paired_analysis.json`: paired descriptive metrics and bootstrap intervals.
- `manifest.snapshot.json`: frozen matrix snapshot; its Git-normalized LF form is semantically identical to the CRLF working-tree bytes whose SHA-256 is recorded in `run_metadata.json`.
