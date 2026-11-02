"""A narrowly scoped unauthorized-access probe for the Continuity arm."""
from __future__ import annotations

import json
import os
import secrets

import httpx

from continuity.contracts import Denied, Intent
from continuity.db import RuntimeDB
from continuity.runtime import ContinuityRuntime

from .shared import CASE_REF, PROVIDER_NAME, SOURCE_REF


def main() -> int:
    if os.name != "posix" or os.getuid() == 0:
        raise RuntimeError("outsider probe must run under its unprivileged UID")
    db = RuntimeDB(os.environ["PHASED_DATABASE_URL"], create_schema=False)
    runtime = ContinuityRuntime(db)
    grants = json.loads(os.environ["PHASED_AGENT_GRANTS_JSON"])
    result = {"actor": "outsider", "old_stream_denied": False,
              "new_authority_denied": False, "owner_control_denied": False}
    try:
        try:
            runtime.get_operation(os.environ["PHASED_A_PACK_OPERATION_ID"], actor="outsider")
        except Denied:
            result["old_stream_denied"] = True
        intent = Intent(
            operation_id="outsider-attempt-" + secrets.token_hex(8),
            principal="shipment-owner",
            provider=PROVIDER_NAME,
            resource_ref=CASE_REF,
            action="quarantine",
            payload={"case_ref": CASE_REF, "quantity": 1},
            authority_id=grants["ship"],
            expected_versions={SOURCE_REF: "v2"},
            effect_key="outsider-attempt:" + secrets.token_hex(8),
        )
        try:
            runtime.submit(intent, actor="outsider", participants=["outsider"])
        except Denied:
            result["new_authority_denied"] = True
        with httpx.Client(trust_env=False, timeout=1.0) as client:
            response = client.post(
                os.environ["PHASED_OWNER_URL"].rstrip("/") + "/v1/owner/grant-agent-b",
                json={},
            )
            result["owner_control_status"] = response.status_code
            result["owner_control_denied"] = response.status_code in {401, 403}
    finally:
        db.close()
    result["denied"] = all(result[key] for key in (
        "old_stream_denied", "new_authority_denied", "owner_control_denied",
    ))
    print("PHASED_OUTSIDER=" + json.dumps(result, sort_keys=True, separators=(",", ":")), flush=True)
    return 0 if result["denied"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
