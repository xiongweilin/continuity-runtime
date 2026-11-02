# PCCT mature-stack counterexample — results

**Conclusion: `NO DIFFERENTIATOR` for the bounded behaviors and source formats tested.** The PostgreSQL + application-policy comparator realizes the stated accountability and authorization checks across both independently encoded sources. It does not reveal a behavior available only through PCCT, nor an adapter-count advantage for the portable profile. This is not a claim that the comparator has lower total LOC or lower real-world maintenance cost; the equivalent production-grade PCCT verifier is not implemented, and human review/maintenance time was not measured.

This is a synthetic local study—not a Runtime feature, proof of global completeness, production security assessment, customer pilot, or ROI study. No GitHub PAT, real provider, business system, production data, or external write was used. The only external research is the cited prior art.

## Provenance and run

| Item | Evidence |
|---|---|
| Frozen implementation commit | `a3394ca5b8385e1a0692af695340ee65b4c4b1e9` (parent: `47fa3d8516f38dd242273f74923a6095541c5762`; PR #19 base `9c417b04116fc27b9dbc5421bab1eeeb63f2c97c`) |
| Implementation PR | [PR #20](https://github.com/xiongweilin/continuity-runtime/pull/20), draft, stacked on PR #19; open and unmerged |
| Results branch base | `main` commit `24332318334c932c2bd7d6133419d806f28801e0` |
| Run | `pcct-20261102T091123Z-c4926e36`; 2026-11-02 09:11:23–09:11:34 UTC |
| Runtime/test versions | Docker Engine 29.9.2; cached image `postgres:17`, image ID `sha256:b8faeff7712663c2773a190181e1246e37e364b887611c04658c28b44e2408fe`; PostgreSQL 17.11; Python 3.12.13; psycopg 3.3.6; cryptography 46.0.7; pytest 9.2.0 |
| Test outcome | **51 passed, 0 failed, 0 skipped**, pytest-reported 3.46 s; full harness about 12 s |
| CI | [Workflow run `41418707254`](https://github.com/xiongweilin/continuity-runtime/actions/runs/41418707254) succeeded for the final commit; repository pytest: 151 passed, 1 warning, 0 skipped (4.48 s) |
| Local static checks | Ruff lint and format passed; Python compile check passed |
| Cleanup | Python test process exit 0; container/volume/network removal exits 0; cleanup verification exit 0; remaining containers, volumes, networks all 0 |

See `final/pytest.log`, `final/pytest.xml`, `final/run-metadata.txt`, `final/environment.txt`, `case_matrix.csv`, and `source-commit.txt`. The test harness used a loopback-published Docker bridge because Docker Desktop cannot publish ports from an internal-only network. The bridge could route outward; the test code made no provider/API calls. All temporary resources were removed.
`artifact_manifest.csv` provides SHA-256 and byte counts for the other published artifacts (it intentionally does not hash itself).

## What the comparator implements

- PostgreSQL relations for source revisions and raw source bytes, signed attestations, open obligations, external effects, grants, transitions, and per-obligation dispositions.
- Primary/foreign keys, checks, and a partial unique index rejecting multiple active grants for one scoped action.
- Serializable validation under a locked source revision row; accepted transition dispositions and activation of the replacement grant commit atomically.
- Application policy verifies Ed25519 signatures and issuer roles, scope/cut/query/revision/digests/expiry, source-owner and effect-ledger completeness assertions, goal-owner authorization, grant authority, old-owner release, recipient acceptance, revocation, and synthetic provider version-fence capability.
- Two separately authored formats normalize to the same bounded records: append-only domain-event JSONL and a workflow-history projection with different event and field names. Each format runs the same 25 test scenarios; one additional test asserts normalized source equivalence.

Every required negative case fails closed: omitted responsibility/effect, duplicate disposition, altered or equivocal evidence, missing/unknown/expired completeness, stale source, unknown effect, missing handoff acceptance, unauthorized supersession, self-issued or stale grants, and absent/unsupported provider fencing. An unaffected action remains eligible when a dependency-bound action is held.

The attestation issuer is the authority for its own bounded assertion. The verifier cannot prove that an owner recorded a never-recorded event: a dishonest issuer can sign a false but internally complete cut, which is indistinguishable from true absence. The appropriate result without a trusted completeness witness is `INCOMPLETE_SCOPE`. Similarly, a signed provider capability fixture is not a live compare-and-set fence; no external write was made.

## Case results

`case_matrix.csv` contains all 51 executed pytest cases and their asserted verdicts. The grouped result is:

| Expected result | Passing cases |
|---|---:|
| `VALID` | 2 |
| `INVALID` | 26 |
| `HOLD` (including cases where unaffected action remains eligible) | 14 |
| `INCOMPLETE_SCOPE` | 8 |
| Cross-format equivalence (not a verdict) | 1 |
| **Total** | **51** |

The 50 paired cases are the 25 adversarial scenarios run against both formats. The extra test compares the normalized source-owned evidence across formats. The JUnit XML is the test-run evidence; the CSV maps each test case to its asserted verdict.

## Complexity and audit effort

Counts below use physical/nonblank source lines at the frozen implementation commit and are scope markers, not direct measures of development or lifetime maintenance effort:

| Component | Count |
|---|---:|
| Domain-event adapter | 68 physical / 64 nonblank lines |
| Workflow-history adapter | 71 physical / 67 nonblank lines |
| Application policy | 618 physical / 586 nonblank lines |
| PostgreSQL schema | 101 physical / 96 nonblank lines |
| Signature verification | 88 physical / 68 nonblank lines |
| Entire research Python/SQL tree | 1,520 nonblank lines, including 421 lines of synthetic issuer/fixture construction |
| Test module | 414 physical lines; 51 executed cases |
| Existing PCCT semantic oracle | 139 physical lines; explicitly no authentication, source adapters, database, freshness proof, or provider enforcement |

The second source format is a separate 71-line adapter; it uses the same normalized ledger and policy and requires no source-specific schema/policy branch. A PCCT profile would also need to map both heterogeneous sources and validate authentic completeness/authority. PR #19 has only a trusted-cut expected-output oracle, so total equivalent PCCT implementation cost cannot be measured from its LOC. The evidence therefore shows **no measured integration/adapter advantage** for PCCT, but does not support a claim that PostgreSQL has lower raw implementation LOC.

The six-step [reviewer checklist](reviewer_checklist.md) covers raw cut/digest; issuer/completeness; obligation rows; effect index/readback; goal/grant/release/acceptance; and committed transition/verdict. No independent human reviewer performed timed reconstruction: `operator_seconds = null`; maintenance effort and dollar cost are also null.

## Bounded lifecycle decision

- **Prior art:** generalized commitment machines already formalize commitment-based communication protocols ([IJCAI 2007](https://ijcai.org/proceedings/2007/245)); composite commitment verification under partial observability is studied in ([IJCAI 2015](https://ijcai.org/proceedings/2015/9)); maintenance commitments are formalized in ([AAAI 2021](https://ojs.aaai.org/index.php/AAAI/article/view/17355)). No novelty claim is made for commitments, delegation, goal maintenance, or handoff.
- **Evidence formats do not prove completeness by themselves:** RFC 9162 specifies Certificate Transparency log artifacts and consistency evidence, not the completeness of a business obligation register ([RFC 9162](https://www.rfc-editor.org/rfc/rfc9162.html)). The in-toto Bundle spec says the bundle is not authenticated as a whole and discusses deletion/replay risks ([in-toto Bundle spec](https://github.com/in-toto/attestation/blob/main/spec/v1/bundle.md)). The comparison therefore relies on explicitly trusted bounded completeness attestations in both arms.
- **Research state:** reject the independent Runtime/product differentiator for these tested semantics (`NO DIFFERENTIATOR`). The conventional stack performs the required bounded checks with common source-owner adapters, familiar relational constraints, and application policy; no PCCT-only semantic behavior or portability win was demonstrated.
- **Not established:** lower maintenance or review time; source-owner truthfulness/global completeness; real provider fencing; cross-organizational governance; a production verifier; or business ROI.
- **Revalidation condition:** reopen only with a concrete, independently authenticated source format/profile that demonstrates materially fewer source-specific adapters or reviewer reconstruction steps under the same completeness and authorization assumptions. Do not infer a Runtime requirement from a missing implementation or from this synthetic test alone.

## Prior-run failures and redaction

`attempts/` preserves sanitized harness logs and cleanup metadata for the four implementation failures: internal Docker-network port publishing, project import path, Psycopg 3 batch API, and frozen rollback exception. All four resource cleanups succeeded. `redaction_manifest.csv` maps published attempt files to original local log hashes; only local user-profile paths and the local host name were redacted. The early harness run IDs used local wall time with a `Z` suffix; those IDs are not treated as UTC. Final run timestamps are generated from UTC and recorded in `final/run-metadata.txt`.

The results PR contains only this report and experiment evidence under `results/pcct_counterexample/`; no prior Phase C/D results or implementation source is copied into it.
