from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256
from typing import Any

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)

from .model import SignedStatement


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def digest(value: Any) -> str:
    return sha256(canonical_bytes(value)).hexdigest()


def utc_text(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def signed_statement(
    issuer_id: str,
    kind: str,
    payload: dict[str, Any],
    private_key: Ed25519PrivateKey,
) -> SignedStatement:
    message = canonical_bytes({"issuer_id": issuer_id, "kind": kind, "payload": payload})
    return SignedStatement(issuer_id, kind, payload, private_key.sign(message))


@dataclass(frozen=True)
class TrustedIssuer:
    public_key: Ed25519PublicKey
    roles: frozenset[str]


@dataclass(frozen=True)
class TrustStore:
    issuers: dict[str, TrustedIssuer]
    source_owners: dict[str, str]

    def verify(
        self,
        statement: SignedStatement,
        expected_role: str,
        now: datetime,
    ) -> str | None:
        issuer = self.issuers.get(statement.issuer_id)
        if issuer is None:
            return "unknown_issuer"
        if expected_role not in issuer.roles:
            return "issuer_role_not_authorized"
        message = canonical_bytes(
            {
                "issuer_id": statement.issuer_id,
                "kind": statement.kind,
                "payload": statement.payload,
            }
        )
        try:
            issuer.public_key.verify(statement.signature, message)
        except InvalidSignature:
            return "bad_signature"
        if statement.payload.get("expires_at"):
            if parse_time(statement.payload["expires_at"]) <= now:
                return "expired_statement"
        return None


def public_key_bytes(key: Ed25519PublicKey) -> bytes:
    return key.public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw,
    )
