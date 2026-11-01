# Phase C experiment data

This branch contains **data and report artifacts only**; the local runner, mock provider, fault proxy, independent PostgreSQL+DBOS baseline, and evaluator are in implementation PR #5 at commit `04f21de2312a9042a81452b16e74ace2edfe8c36`.

- `full-160/` is the only scored run: 8 frozen scenarios × 10 seeds × 2 arms. Its evaluator input is `full-160/trials.jsonl` + `full-160/provider_oracle.jsonl`; it passed structural/safety checks but returned `EVIDENCE_INCOMPLETE` because infrastructure cost is null.
- `full-160/REPORT.md` explains results, uncertainty, costs, scope, and limitations. `paired_analysis.json` contains paired per-scenario and overall summaries.
- `full-160/provider_ledgers/`, `fault_proxy_logs/`, and `trial_diagnostics/` preserve raw per-trial provenance and cleanup evidence. `artifact_manifest.json` records committed Git-blob hashes/sizes (excluding itself); verify against repository bytes, since Windows `autocrlf` may change checkout line endings.
- `aborted-development/` preserves two aborted development attempts, including a non-scored partial overlap-barrier failure. Those partial files are **not** evaluator input and must not be treated as a complete dataset.

No real GitHub or production-provider writes were performed. The full run used a local simulated provider on an internal Docker network. No claim of production safety or positive ROI is made.
