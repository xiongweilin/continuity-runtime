# PostgreSQL + application-policy counterimplementation

This is a bounded synthetic research comparator, not a Runtime, production service, or provider executor. It receives source-owner cuts, commits obligations/effects/grants to PostgreSQL, validates signed claims and domain policy in the application, then records an accepted transition in one serializable transaction. PostgreSQL keys, foreign keys, checks, and a partial unique index constrain the stored state.

## Trust and evidence boundary

- Test-only Ed25519 keys are generated in memory for every case. The database stores signed statements and public test data only; private keys are neither persisted nor printed.
- A configured source-owner key attests the source ID, bounded query/cursor, revision, raw digest, policy revision, complete obligation count/digest, and expiry. A separate effect-ledger key attests the complete external-effect index; an authority key signs the complete grant register; a goal owner signs the exact supersession decision; the old owner and recipient sign release/acceptance; and a synthetic-provider key attests version-fenced write capability. All keys are test-only and generated in memory.
- The verifier rejects an absent/unknown/expired completeness claim as `INCOMPLETE_SCOPE`, altered signatures and equivocation as `INVALID`, a stale current revision or unknown side effect as `HOLD`, and valid transitions as `VALID`.
- A signature proves which configured key signed a claim and detects later alteration. It does **not** prove the owner recorded every real-world obligation or effect. If the source owner signed a false but internally complete cut, both PCCT and this comparator can be fooled. This fixture does not test owner governance or external query-completeness proofs.
- The provider capability is only a signed synthetic fixture. No provider API is invoked and no actual conditional write is made.
- The source cut is checked under a PostgreSQL serializable transaction with the current revision row locked. No provider write is made. A later external write would still require a provider-enforced version fence; this study does not establish such a fence.

## Independent source adapters

`adapters/domain_events.py` reads an append-only, sequence-numbered domain event JSONL stream. `adapters/workflow_history.py` reads a separately authored workflow-execution projection with different field names and event representation. Both are synthetic upstream formats, not translations of Continuity tables. The fixtures encode the same scope, two open obligations, one confirmed historical effect, goal supersession, old grant revocation, one pending replacement grant, and one unrelated active grant.

## Run

With Docker and a cached `postgres:17` image available, run from the repository root:

```powershell
./scripts/run_pcct_counterexample.ps1
```

The harness creates a temporary Docker bridge network, a named volume, and a PostgreSQL 17 container published only on loopback. The bridge can route outward, but the test program makes no provider or other external calls. It records test and cleanup exit codes in a new `%LOCALAPPDATA%\ContinuityRuntime\PcctResearch\...` directory, then removes the container, volume, and network. It does not pull images or use GitHub credentials, provider accounts, or external writes.

The PostgreSQL tests also accept the repository CI variable `CONTINUITY_TEST_POSTGRES_URL`; they create a unique per-test schema and drop only that schema. The standalone harness sets `CONTINUITY_PCCT_TEST_DSN` instead.

## Interpretation

The application policy is intentionally direct: one record per obligation, one authorized disposition for every open obligation, preservation of every prior effect, fresh owner-signed goal decision, a role-authorized replacement grant, old-owner release, recipient acceptance, and isolation of unrelated actions. No code is imported from `research/pcct/semantic_oracle.py`; that file is an expected-output oracle only.

Passing synthetic cases shows that the listed behavior can be represented with a conventional PostgreSQL ledger, constraints, signed attestations, and application validation. It does not prove globally complete data, real-source interoperability, cryptographic deployment security, or lower operational cost. The results report separates these findings from the unmeasured human maintenance effort and the absent production-grade PCCT verifier.
