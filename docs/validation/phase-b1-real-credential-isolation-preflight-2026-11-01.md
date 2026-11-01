---
title: Phase B1 real-credential isolation preflight
date: 2026-11-01
code_commit: 298b22e48cdfbbe9a89bc137155a52b486350e49
status: passed-with-documented-exit-code-anomaly
---

# Phase B1 real-credential isolation preflight

## Scope

Read-only credential-scope checks, local secret-placement/network isolation checks, and Runtime operation registration only. No /dispatch, GitHub label write, or other provider mutation was performed. No owner gh login was used. Secret values are omitted.

## Observed

- The PAT file and independently generated executor-key file were outside the checkout. Their directory DACL was protected; both files granted access only to the current user and SYSTEM. File contents were never emitted.
- The user-provided GitHub Settings screenshot for token continuity-phase-b-test shows: resource owner xiongweilin; expiration 2026-11-08; repository access limited to the selected repository xiongweilin/continuity-runtime-phase-b-sandbox; no user permissions; repository permissions are Metadata read and Issues read/write. No other optional write permission is listed. The screenshot did not expose the token value.
- GitHub documents that fine-grained PATs retain read-only access to public repositories, so this selection limits private repository access but is not an all-repositories denial for public reads. See https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/managing-your-personal-access-tokens.
- Using the PAT only inside trusted-executor, GET /user/repos?visibility=private returned one private repository: the Phase B sandbox. GET for Issue #1 returned 200 with zero labels. The repository Contents read probe and GitHub Actions permissions read probe returned 403.
- The collaborator-list read endpoint returned 200. GitHub documents this endpoint as requiring Metadata read; this result is not evidence of Administration permission. See https://docs.github.com/en/rest/collaborators/collaborators#list-repository-collaborators.
- Compose config and live container inspection showed:
  - API mounted the executor key only; it had no GitHub token file mount or GitHub-token environment variable.
  - Trusted executor mounted the PAT and executor key, and both files were readable there. It did not receive the synthetic Runtime admin/agent bearer variables.
  - Agent probe had no secret mounts or privileged environment variables. It resolved the Runtime API but not trusted-executor.
  - API networks were project default and effects; executor networks were effects and provider-egress; the executor had no published port. The Agent probe was on default only.
  - An additional Agent TCP attempt to the executor's effects-network IP on port 9090 was blocked.
- Distinct synthetic Runtime admin and agent identities were used. The preparation script output reported operation phase-b-sandbox-issue-1-label-operation as registered, dispatch_generation=0, duplicate logical effect rejected, and provider_writes=0. A follow-up local GET confirmed the operation remained registered with generation 0.
- Exit-status investigation: the script's success path calls main() without an explicit nonzero exit after printing. A no-network mocked success path through the same PowerShell cleanup wrapper returned child exit code 0; controlled PowerShell tests also continued after a native nonzero exit and captured it correctly. These checks did not reproduce the original outer exit code 1.
- The original PowerShell invocation did not separately record the Python child's exit code before cleanup, so the precise source of that single exit code 1 remains indeterminate. The operation state was independently verified by local GET; the fixed-ID preparation script was not rerun.
- After cleanup, project continuity-phase-b had zero containers, volumes, and networks; port 18086 was free. The sandbox Issue remained open with zero labels.

## Limitations / not tested

- The configured PAT repository and permission settings are verified from the supplied Settings screenshot and corroborated by read-only API probes. No Issues write was attempted, by design; configuration was verified, but actual write behavior was not tested.
- Contents and Actions read probes were denied. No provider effect, dispatch, or label-write capability test was performed.
- These are observed results from one local Docker setup, not a production or host-level non-bypassability certification.
- Compose build images remain in the local cache; containers, networks, and database volume were removed.
