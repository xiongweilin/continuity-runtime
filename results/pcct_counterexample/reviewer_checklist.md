# Reviewer trace reconstruction checklist

This is a reproducible review recipe, not a claim that a human completed or timed it. Each test case can be checked in these six steps:

1. Open the selected source-format fixture and verify the source/cut/query/revision identity and raw SHA-256 in the signed source-cut statement.
2. Verify configured issuer identity/role, signature, expiry, and the source owner's explicit completeness claims for obligations; inspect separate signed effect-index and authority-register completeness.
3. Compare the enumerated `open_obligations` rows against the signed count/digest, then compare each proposal disposition to the exact obligation ID and old owner.
4. Compare `external_effects` and readback references against the signed effect index; preserve `unknown` as HOLD and verify every effect key is retained in the transition.
5. Verify goal-owner supersession scope, old-owner release, recipient acceptance, authority-owner grant scope/revision/status, revocation, and the signed synthetic provider capability where required.
6. Inspect the serializable transaction result, transition/disposition rows or rollback, grant activation status, and eligible independent actions; confirm the asserted verdict in `case_matrix.csv` and `final/pytest.xml`.

No reviewer performed timed reconstruction. Human operator/review seconds are therefore null.
