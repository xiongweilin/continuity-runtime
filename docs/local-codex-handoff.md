# Local Codex handoff: isolated reality-side acceptance

**Status:** This document describes work that needs access to a real isolated
execution environment. Repository CI alone cannot establish provider credential
isolation, genuine remote readback, or all network/crash interleavings.

## Authorized scope

Use a **throwaway** test workspace, a test GitHub repository/issue, and a
least-privileged GitHub App installation or fine-grained token limited to Issues
label writes on that test repo. No production systems, broad account tokens,
actual customer records, live canary traffic, irreversible business effects,
or private keys in Git, logs, terminal transcripts or ChatGPT.

**Do not perform external writes until the test repository, write scope,
and isolation boundary have been explicitly selected by the owner.**

## Phase A — reproduce without real credentials

1. Clone the repository and pin a known passing main commit; record its SHA.
2. With Python 3.12+ and Docker, run:
   ```bash
   python -m pip install -e '.[dev,dbos,github]'
   python -m pytest -q
   python examples/canary_guarded_handoff.py
   ```
3. Use local test-only environment variables for Compose:
   ```bash
   export POSTGRES_PASSWORD='<random_local_test_password>'
   export CONTINUITY_TOKENS_JSON='{"ci-admin":{"actor":"ci-root","admin":true},"ci-agent":{"actor":"ci-agent"}}'
   docker compose up --build -d --wait
   python scripts/smoke_compose.py
   docker compose down -v
   ```
   The smoke script and this example intentionally use matching CI-only
   fixture tokens. They are not production credentials or production IAM.

## Owner-approved disposable repository setup (2026-11-01)

The owner has explicitly authorized **creating one new disposable GitHub test
repository solely for Issue-label effect experiments**. This permission covers
isolated setup (creating the repository, one test Issue and one disposable label);
it does **not** authorize production writes, broader credentials, unrestricted
network access, other GitHub mutations, or claiming the test is production-ready.

Suggested private repository: `xiongweilin/continuity-runtime-phase-b-sandbox`.
Use an alternative unique *disposable* suffix if that slug already exists;
never overwrite, delete or repurpose an existing repository.

**Local Codex setup, with owner-authenticated `gh` CLI:**

```powershell
gh auth status
gh repo view xiongweilin/continuity-runtime-phase-b-sandbox
# If the repository does NOT exist, create it once:
gh repo create xiongweilin/continuity-runtime-phase-b-sandbox --private --add-readme --description "Disposable Phase B Issue-label effect mediation test only"
gh issue create --repo xiongweilin/continuity-runtime-phase-b-sandbox --title "Phase B disposable label effect" --body "Synthetic, non-sensitive Issue for isolated effect-coordination experiments only."
gh label create continuity-effect-test --repo xiongweilin/continuity-runtime-phase-b-sandbox --color 0E8A16 --description "Disposable label for Phase B effect experiment"
```

The `gh repo view` check may exit nonzero when the repository does not exist;
that is expected. **Never blindly run `gh repo create` or `gh issue create`
twice**; first check whether setup has already succeeded. If creation fails due
to CLI auth or repository permissions, report the blocker rather than using
a broad token in source files.

Do not apply the label to the test Issue as part of setup: **that write is the
actual effect to test later**. Report the created repository URL, Issue URL and
label name without any secret. The owner-authenticated `gh` CLI used for
repository setup must not be exposed inside the proposing-Agent container.
Before the first real Issue-label effect dispatch, separately verify a
least-privileged executor credential, Agent credential exclusion, and route
isolation. Do not attempt credential creation or effect dispatch silently.


## Phase B local credential isolation and read-only preflight

Verified sandbox target (reported by local Codex and inspected via GitHub):

- Repository: `xiongweilin/continuity-runtime-phase-b-sandbox` — private
- Target: Issue `#1` — synthetic, no labels at setup
- Allowed label: `continuity-effect-test`
- The broad `gh` CLI owner login is **setup-only**; never copy its token to
  the executor, Compose environment or Agent.

**Important GitHub permission limit:** Fine-grained token permission
`Issues: read and write` is wider than “label-write-only.” GitHub does not
offer a standalone Issue-label-only permission. Minimize the token to **only
this repository**, do not grant Contents/Administration/Workflows permissions,
and enforce the exact Issue/label allowlist again in the executor. The
`Metadata: read` permission is standard. Do not claim the token itself is
label-only.

### Stage B0 — run now, without any real credential

The owner-selected GitHub fine-grained token is **not** a prerequisite for
the **offline** network/secret/operation test. From the repository root, with
Docker Desktop running and port 18086 free:

```bash
python scripts/phase_b_offline.py
```

This script creates **invalid synthetic** provider credentials and a separate
temporary test-only service key outside the checkout, runs the same Phase B
Compose overlay, checks Agent isolation and logical-effect deduplication, then
automatically removes the test containers and volumes. It does not issue
`/dispatch`, read the private GitHub issue, or attempt any real provider
effect. It also omits any preexisting `GH_TOKEN` or `GITHUB_TOKEN` inherited
from the shell. Its success **does not validate real PAT permissions or
the future live executor token file ACL**.

If Docker is unavailable, report that prerequisite instead of asking for a
GitHub token. If Docker-based cleanup fails, report the remaining resources
by their `continuity-phase-b-offline` Compose project name rather than
blindly pruning unrelated volumes.

### Stage B1 — real credential preflight, still no write

Local Codex should perform **preflight without external writes**:

1. Have the owner create a repository-selected fine-grained PAT (or
   repository-scoped GitHub App installation token) permitting Issues
   read/write and no other writable repository permission. Store it in a
   filesystem secret file **outside** the Git working tree, not in a terminal
   transcript, chat, `.env` or command argument. The owner-only CLI credential
   must remain outside all application containers.
2. Generate a **different**, independent random service key (at least
   32 characters), also in an external secret file. Neither file may be
   mounted into an Agent container.
3. In the local shell, set `POSTGRES_PASSWORD`,
   `CONTINUITY_TOKENS_JSON` with separate synthetic admin and
   `phase-b-agent` identities, `CONTINUITY_PHASE_B_TOKEN_FILE` and
   `CONTINUITY_PHASE_B_EXECUTOR_KEY_FILE` to those **two secret-file paths**.
   Set `CONTINUITY_PHASE_B_ADMIN_BEARER` and
   `CONTINUITY_PHASE_B_AGENT_BEARER` to the corresponding **Runtime test
   bearer strings**, **not** the provider token.
4. Run the overlay without any real effect dispatch:

   ```bash
   docker compose -p continuity-phase-b -f compose.yaml -f compose.phase-b.yaml up --build -d --wait
   docker compose -p continuity-phase-b -f compose.yaml -f compose.phase-b.yaml --profile isolation-test run --rm agent-isolation-probe
   python scripts/phase_b_prepare.py
   ```

   The preparation script issues an exact scoped grant, registers one
   `registered` operation and confirms a second operation with the same
   logical `effect_key` is rejected. It deliberately never calls
   `/dispatch`. Verify the target Issue still has **zero labels**.
5. Observe the sanitized outputs, check `docker compose config` for
   network/secret placement without printing secret file contents, check
   separately that the Agent cannot read or call the trusted executor, and
   confirm the Runtime container has **no GitHub token secret**.
6. Stop here and report the credential's repository selection, permission
   names, agent/executor network probe outcome, registered operation ID,
   target Issue readback and container cleanup state. Do not paste actual
   credential text. Use
   `docker compose -p continuity-phase-b -f compose.yaml -f compose.phase-b.yaml down -v`
   to remove test containers and test volumes.

**Only after independent review of those results** is the one test-label
write eligible for a separately authorized dispatch. Do not use `gh issue
edit --add-label` as a shortcut; the write must pass through Continuity's
Effect Coordinator and the trusted executor.

## Phase B — credential and effect mediation (requires local environment)

1. Run the **trusted executor** in a distinct process/container, holding the
   test-provider write credential. Proposing Agent processes must have **no**
   provider write credentials. Check containers' environment, mounted secrets,
   permissions and direct network routes for bypass.
2. Configure `GitHubLabelAdapter` with an exact test repository and
   explicit label allowlist. Use one pre-created test issue and one disposable
   label. Provider credential never appears in operation payload or API response.
3. Issue a scoped Runtime grant. Submit the **same logical effect_key** from
   two independent Agent/workflow execution paths with different operation IDs;
   one reservation must be rejected.
4. Dispatch the winner through the trusted executor. Prove a direct Agent write
   attempt fails. Confirm the label through provider GET, then have the domain
   verifier separately assess the goal.
5. Inject failures after effect reservation, during provider request and after
   provider success but before receipt persistence. Use safe test doubles or a
   controllable proxy where actual remote ambiguity cannot be staged safely.
   UNKNOWN must never cause a blind duplicate effect.
6. Revoke authority, change a protected source version, or remove the
   required admission checker before dispatch. All paths must be fail-closed.
7. Repeat one comparable flow without Continuity using simple PostgreSQL +
   DBOS + scripts. Measure *verified outcomes, human interventions, retries,
   runtime overhead and total task costs*; do not infer productivity from
   test pass counts.

## Evidence / reporting contract

Record only sanitized artifacts: Git commit, integration scenario identifiers,
test outcomes, external request IDs with non-sensitive identifiers, failure
injection points, version transitions, outcome evidence type and resource/time
costs. Never post tokens, cookies, signed headers, sensitive issue contents or
private service addresses.

Store a concise report in `docs/validation/` or a repository issue/PR.
Each result must distinguish **observed**, **inferred**, **simulated** and
**not tested**. Do not upgrade the project to production-ready on the basis of
one disposable GitHub label test.

## Stop and ask for further authorization

A production credential, higher-impact provider action, public network exposure,
unrestricted Agent network access, a second real business domain, or sustained
unattended execution needs separate scope/approval. If trusted credential
isolation or reliable independent readback cannot be established, remain in
read-only/dry-run mode and document the blocker.
