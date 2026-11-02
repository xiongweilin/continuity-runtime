# Development attempt log

These attempts are preserved to record environmental/implementation corrections; they are not acceptance runs. Each corresponding pytest/harness log and resource exit record is in the sibling numbered directory. All temporary Docker resources were removed after every attempt.

| Attempt | Observed UTC start (file metadata) | Result | Cause and correction | Cleanup |
|---|---|---|---|---|
| 001 — Docker port | 2026-11-02 08:21:58Z | Tests not started; Python exit 125 | Docker Desktop would not publish a port from an `--internal` network. Switched to a loopback-only published bridge; final test program makes no provider/API calls. | Container, volume, network removal 0; verification 0 |
| 002 — project import | 2026-11-02 08:22:42Z | Pytest exit 4 during collection | Isolated `uv --no-project` environment could not import the repository’s `src/continuity`; harness now supplies `PYTHONPATH=src` and the project runtime dependencies. | All removals 0; verification 0 |
| 003 — Psycopg batch API | 2026-11-02 08:33:51Z | 38 failed before policy assertions | Psycopg 3 exposes `executemany` on a cursor, not `Connection`; changed repository writes to use a cursor. | All removals 0; verification 0 |
| 004 — rollback exception | 2026-11-02 08:35:12Z | 2 passed, 36 failed in expected-rejection paths | A frozen dataclass exception could not accept the traceback assigned by contextlib, masking the expected rollback verdict. Replaced it with a normal exception class. | All removals 0; verification 0 |
| 005 — pre-CI-DSN-fallback run | 2026-11-02 09:00:12Z | 51 passed, 0 skipped | Local harness DSN was configured; later CI review found the repository-standard DSN alias was missing. The test was updated to consume that alias; final run 006 is bound to the updated commit. | All removals 0; verification 0 |

After attempt 004, development runs passed, then the implementation was expanded to separate effect/authority/provider attestations, generalized for both adapters, frozen at `47fa3d8…`, and passed 51 cases. A follow-up enabled the shared CI PostgreSQL DSN; attempt 006 in `../final/` is the final 51-case run bound to commit `a3394ca…`.

The original run IDs from attempts 001–004 were generated before the UTC timestamp fix and include a local-time string suffixed with `Z`. Use filesystem creation timestamps above for ordering; do not use the old run IDs for UTC duration claims. Local user-profile paths are redacted in the published copies; source and published hashes are recorded in `../redaction_manifest.csv`.
