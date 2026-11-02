# Phase D results

This results-only change records the isolated Phase D run from implementation commit
`f8eb9393eeabd12c893ea0f0845cc9e7fabc2bcd` (implementation PR #13), using the frozen
protocol/design in PR #11. It contains no implementation changes.

## Full run

- Runner ID: `phased-5c11577697ae4e08`; UTC window 2026-11-02 06:23:44–06:26:02; source commit `f8eb9393eeabd12c893ea0f0845cc9e7fabc2bcd` was clean at launch.
- 24/24 exact scenario × seed × arm rows; 12 paired comparisons.
- Full evaluator ran with exit code 0 and returned **`EVIDENCE_INCOMPLETE`**.
- Both arms: 12 trials, 9 satisfied and 3 unresolved (the three expected pre-send crash cases).
- Evaluator reported zero safety violations and zero functionality gaps.
- Median / p95 elapsed time: Continuity 2,920.34 / 3,844.47 ms; PostgreSQL/DBOS 3,054.72 / 3,846.58 ms.
- Database transactions: 5,193 Continuity; 5,400 PostgreSQL/DBOS.
- Scripted reconstruction checklist: 60 steps per arm (5 per trial); human operator time was not observed.
- Model cost: $0. Infrastructure cost and operator seconds are null; the evaluator therefore withholds a complete cost/ROI verdict.

The frozen evaluator and its rules were not changed. No positive ROI, production reliability, or production-safety claim is made. The evaluator validates recorded claims and cross-source consistency; reviewers should inspect the independent provider, source, goal/authority, proxy, barrier, process, and Runtime/DBOS artifacts.

## Development and isolation

`development-seed0/` contains the final one-seed development run (8 trials, evaluator intentionally not run) and its host cleanup metadata. The full-run preflight passed **115 tests** (one upstream Starlette/httpx deprecation warning) and the arm-UID isolation probe passed. The Docker network was internal with no published ports; the host wrapper recorded zero remaining containers, volumes, and networks after cleanup. No live provider, GitHub PAT, external provider write, or egress was used in the experiment.

The run metadata documents an important boundary: Agent A and Agent B share a per-trial application database credential. Runtime actor scoping and the Phase D wrappers are application-level controls, not arbitrary-SQL or RLS isolation.

## Files

- `full-24/`: exact evaluator inputs/output, run and host metadata, artifact manifest, trial evidence, and per-trial raw logs/snapshots.
- `development-seed0/`: final unscored development shakedown and raw evidence.
- `attempts/`: preserved aborted setup/shakedown attempts and the earlier successful development iteration. None of these attempts contributes to the 24-row acceptance dataset.
- `artifact_manifest.csv`: SHA-256 and byte counts for every file in this results subtree except the manifest itself.

Implementation PR: [#13](https://github.com/xiongweilin/continuity-runtime/pull/13). Design PR #11 and tracking issue #12 remain open; no merge or issue closure is implied.
