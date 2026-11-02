from __future__ import annotations

from hashlib import sha256

from psycopg import Connection

from .crypto import canonical_bytes
from .model import SourceCut
from .policy_digest import POLICY_SHA256


def execute_many(conn: Connection, statement: str, parameters: list[tuple]) -> None:
    with conn.cursor() as cursor:
        cursor.executemany(statement, parameters)


def persist_cut(conn: Connection, cut: SourceCut) -> None:
    conn.execute(
        """INSERT INTO source_revisions
           (scope, cut_id, source_id, source_format, query_id, cut_revision,
            current_revision, sequence_start, sequence_end, raw_sha256, raw_evidence,
            policy_sha256)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        (
            cut.scope,
            cut.cut_id,
            cut.source_id,
            cut.source_format,
            cut.query_id,
            cut.revision,
            cut.current_revision,
            cut.sequence_start,
            cut.sequence_end,
            cut.raw_sha256,
            cut.raw_bytes,
            POLICY_SHA256,
        ),
    )
    for statement in cut.attestations:
        signed_payload = canonical_bytes(
            {
                "issuer_id": statement.issuer_id,
                "kind": statement.kind,
                "payload": statement.payload,
            }
        )
        conn.execute(
            """INSERT INTO attestations
               (scope, cut_id, kind, issuer_id, payload_bytes, payload_sha256, signature)
               VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            (
                cut.scope,
                cut.cut_id,
                statement.kind,
                statement.issuer_id,
                signed_payload,
                sha256(signed_payload).hexdigest(),
                statement.signature,
            ),
        )
    execute_many(
        conn,
        """INSERT INTO open_obligations
           (scope, cut_id, obligation_id, owner_id, required_action)
           VALUES (%s, %s, %s, %s, %s)""",
        [
            (cut.scope, cut.cut_id, item.obligation_id, item.owner, item.action)
            for item in cut.obligations
        ],
    )
    execute_many(
        conn,
        """INSERT INTO external_effects
           (scope, cut_id, effect_key, disposition, readback_ref)
           VALUES (%s, %s, %s, %s, %s)""",
        [
            (cut.scope, cut.cut_id, item.effect_key, item.status, item.readback_ref)
            for item in cut.effects
        ],
    )
    execute_many(
        conn,
        """INSERT INTO grants
           (scope, cut_id, grant_id, actor_id, action, evidence_revision, status, issuer_id)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
        [
            (
                cut.scope,
                cut.cut_id,
                item.grant_id,
                item.actor,
                item.action,
                item.revision,
                item.status,
                item.issuer_id,
            )
            for item in cut.grants
        ],
    )
