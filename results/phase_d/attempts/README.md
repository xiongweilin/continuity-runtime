# Preserved development attempts

Only `full-24/` is the frozen 24-row acceptance dataset. Aborted attempts below are retained for audit and are not combined with, substituted for, or counted in that dataset.

1. `attempt-001-powershell-rng/`: host wrapper stopped before creating an output folder because this Windows PowerShell/.NET runtime lacks `System.Convert.ToHexString`; no container or trial started. The wrapper was changed to use `BitConverter`.
2. `attempt-002-admin-url/`: runner failed before preflight/trials because psycopg received SQLAlchemy's `postgresql+psycopg` URL scheme. `_admin_conn` now converts it to the native `postgresql://` conninfo form.
3. `attempt-003-oracle-log-event-name/`: one normal Continuity trial ran and its raw ledgers were copied, but the export aborted because the runner expected `upstream_request_started` while the proxy ledger uses `upstream_attempt_started`. The exporter was corrected; the partial run remains unscored.
4. `attempt-004-dbos-app-name/`: the Continuity arm completed; the PostgreSQL/DBOS worker failed its startup barrier because its DBOS app name exceeded DBOS's 30-character limit. The app names were shortened; the partial run remains unscored.
5. `attempt-005-tests-examples-copy/`: the expanded full-suite preflight stopped during collection because `examples/` was omitted from the image. The Docker context/image now includes it; no trial started.
6. `iteration-006-development-35-tests/`: an earlier complete 8-trial development iteration passed the focused 35-test preflight; it is preserved as a non-acceptance development run. The final development run later passed the full 115-test suite.

The first bootstrap failure had no generated run folder; `attempt-001-powershell-rng/failure.json` records the observed error and explicitly states that no trial data existed.
