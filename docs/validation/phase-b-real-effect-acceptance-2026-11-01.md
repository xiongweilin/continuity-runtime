---
title: Phase B first real Effect acceptance
date: 2026-11-01
code_commit: 298b22e48cdfbbe9a89bc137155a52b486350e49
status: passed
---

# Phase B first real Effect acceptance

## Scope and authorization

Owner-authorized single real write, limited to repository xiongweilin/continuity-runtime-phase-b-sandbox, Issue #1, label continuity-effect-test. No other repository, issue, label, or provider write was attempted. No owner gh credential or CLI was used. Secret values and the token description/expiration date are omitted.

## Preflight observed

- The supplied GitHub Settings screenshot showed the fine-grained PAT restricted to the selected sandbox repository, with no user permissions and only Metadata read plus Issues read/write; no other optional write permission was listed. The token has a short-lived expiration. Read-only PAT enumeration returned exactly one private repository, the target sandbox. GitHub's standard fine-grained PAT behavior still includes read-only access to public repositories.
- PAT read-only scope probe: private repository list 200 (target count 1, other private repositories 0); Issue GET 200 with zero labels; target label GET 200; Contents and Actions permission probes 403.
- Secret files were outside the checkout with ACL limited to the current user and SYSTEM. Compose configuration and live inspection showed the PAT and executor key mounted only into trusted-executor; API mounted the executor key only; Agent had no secrets. Agent network probe found no trusted-executor DNS and the direct TCP attempt to executor port 9090 was blocked.
- Before dispatch the local operation GET returned status registered and dispatch_generation 0. The independent GitHub GET showed Issue #1 open with no labels.

## Evidence, kept distinct

### 1. Runtime write receipt

Exactly one POST to Runtime /v1/operations/phase-b-sandbox-issue-1-label-operation/dispatch was issued. It returned HTTP 200, operation status pending_verification, dispatch_generation 1, and receipt_ref github:label-response:xiongweilin/continuity-runtime-phase-b-sandbox/1/continuity-effect-test. The Python dispatch child exit code was 0. This is the Runtime/executor response receipt; by itself it is not independent proof of the external effect.

### 2. Independent effect confirmation

A separate GitHub GET of Issue #1 returned the target label continuity-effect-test and the Issue remained open. The returned updated_at was 2026-11-01T09:44:27Z. A subsequent Runtime /reconcile returned HTTP 200, status effect_confirmed, effect_disposition present, with evidence_ref github:label-readback:xiongweilin/continuity-runtime-phase-b-sandbox/1/continuity-effect-test. The reconcile Python child exit code was 0.

### 3. Business-goal verification

Runtime /verify-outcome returned HTTP 200 with status effect_confirmed, effect_disposition present, outcome_disposition satisfied, and evidence_ref github:label-goal:phase-b-sandbox-issue-1-label-operation. The verify Python child exit code was 0. A final local GET confirmed dispatch_generation 1 and the same receipt, effect, and outcome evidence.

## Retry and anomaly handling

- No timeout, UNKNOWN response, or lost dispatch response occurred. The dispatch endpoint was not called again.
- A preliminary PowerShell Invoke-RestMethod read-only state GET returned 401 unknown bearer. No state was changed by that GET. The same read was then performed with Python urllib and returned registered/generation 0, exit code 0 before dispatch.
- Python child exit codes: prepare 0; dispatch 0; reconcile 0; verify-outcome 0; final state GET 0. Agent isolation probe exit 0. Compose up exit 0 and cleanup exit 0.

## Cleanup and limits

- Compose project continuity-phase-b cleanup completed. Post-cleanup checks found zero containers, volumes, and networks; port 18086 was free. Build images remain in the local cache.
- This validates one disposable GitHub Issue-label effect only. It does not establish production readiness or general exactly-once behavior. The GitHub permission Issues read/write is broader than label-only; the trusted executor's repository/issue/label allowlist is the additional scope boundary.
- Issue #1 is open and currently has the single target label. No other labels or comments were added by this run.
