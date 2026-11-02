CREATE TABLE source_revisions (
    scope TEXT NOT NULL,
    cut_id TEXT NOT NULL,
    source_id TEXT NOT NULL,
    source_format TEXT NOT NULL,
    query_id TEXT NOT NULL,
    cut_revision TEXT NOT NULL,
    current_revision TEXT NOT NULL,
    sequence_start BIGINT NOT NULL CHECK (sequence_start > 0),
    sequence_end BIGINT NOT NULL CHECK (sequence_end >= sequence_start),
    raw_sha256 TEXT NOT NULL CHECK (length(raw_sha256) = 64),
    raw_evidence BYTEA NOT NULL,
    policy_sha256 TEXT NOT NULL CHECK (length(policy_sha256) = 64),
    PRIMARY KEY (scope, cut_id)
);
CREATE TABLE attestations (
    statement_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    scope TEXT NOT NULL,
    cut_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    issuer_id TEXT NOT NULL,
    payload_bytes BYTEA NOT NULL,
    payload_sha256 TEXT NOT NULL CHECK (length(payload_sha256) = 64),
    signature BYTEA NOT NULL,
    FOREIGN KEY (scope, cut_id) REFERENCES source_revisions(scope, cut_id)
        ON DELETE CASCADE
);
CREATE INDEX attestations_lookup ON attestations(scope, cut_id, kind, issuer_id);

CREATE TABLE open_obligations (
    scope TEXT NOT NULL,
    cut_id TEXT NOT NULL,
    obligation_id TEXT NOT NULL,
    owner_id TEXT NOT NULL,
    required_action TEXT NOT NULL,
    PRIMARY KEY (scope, cut_id, obligation_id),
    FOREIGN KEY (scope, cut_id) REFERENCES source_revisions(scope, cut_id)
        ON DELETE CASCADE
);

CREATE TABLE external_effects (
    scope TEXT NOT NULL,
    cut_id TEXT NOT NULL,
    effect_key TEXT NOT NULL,
    disposition TEXT NOT NULL CHECK (disposition IN ('confirmed', 'unknown', 'rejected')),
    readback_ref TEXT,
    PRIMARY KEY (scope, cut_id, effect_key),
    CHECK (disposition <> 'confirmed' OR readback_ref IS NOT NULL),
    FOREIGN KEY (scope, cut_id) REFERENCES source_revisions(scope, cut_id)
        ON DELETE CASCADE
);

CREATE TABLE grants (
    scope TEXT NOT NULL,
    cut_id TEXT NOT NULL,
    grant_id TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    action TEXT NOT NULL,
    evidence_revision TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('active', 'pending', 'revoked')),
    issuer_id TEXT NOT NULL,
    PRIMARY KEY (scope, cut_id, grant_id),
    FOREIGN KEY (scope, cut_id) REFERENCES source_revisions(scope, cut_id)
        ON DELETE CASCADE
);
CREATE UNIQUE INDEX one_active_grant_per_action
    ON grants(scope, cut_id, action) WHERE status = 'active';

CREATE TABLE transitions (
    scope TEXT NOT NULL,
    cut_id TEXT NOT NULL,
    transition_id TEXT NOT NULL,
    old_goal TEXT NOT NULL,
    new_goal TEXT NOT NULL,
    decision_ref TEXT NOT NULL,
    outcome TEXT NOT NULL CHECK (outcome = 'accepted'),
    PRIMARY KEY (scope, cut_id, transition_id),
    FOREIGN KEY (scope, cut_id) REFERENCES source_revisions(scope, cut_id)
        ON DELETE CASCADE
);

CREATE TABLE dispositions (
    scope TEXT NOT NULL,
    cut_id TEXT NOT NULL,
    transition_id TEXT NOT NULL,
    obligation_id TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('retain', 'fulfill', 'supersede', 'transfer', 'hold')),
    from_owner TEXT NOT NULL,
    to_owner TEXT,
    release_ref TEXT,
    acceptance_ref TEXT,
    authority_ref TEXT,
    decision_ref TEXT,
    residual_custodian TEXT,
    outcome_ref TEXT,
    PRIMARY KEY (scope, cut_id, transition_id, obligation_id),
    FOREIGN KEY (scope, cut_id, transition_id)
        REFERENCES transitions(scope, cut_id, transition_id) ON DELETE CASCADE,
    FOREIGN KEY (scope, cut_id, obligation_id)
        REFERENCES open_obligations(scope, cut_id, obligation_id)
);
