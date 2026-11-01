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
