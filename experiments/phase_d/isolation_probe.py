"""One-run preflight proving arm UIDs cannot bypass mock-provider boundaries."""
from __future__ import annotations

import json
import os

import httpx


def _read_denied(path: str) -> bool:
    try:
        with open(path, "rb") as handle:
            handle.read(1)
    except (PermissionError, FileNotFoundError, ProcessLookupError):
        return True
    return False


def main() -> int:
    if os.name != "posix" or os.getuid() == 0:
        raise RuntimeError("isolation probe must run under the arm UID")
    trial_id = os.environ["PHASED_PROBE_TRIAL_ID"]
    headers = {"X-PhaseD-Trial-ID": trial_id, "X-PhaseD-Arm": "continuity"}
    provider_url = os.environ["PHASED_PROVIDER_URL"].rstrip("/")
    proxy_url = os.environ["PHASED_PROXY_URL"].rstrip("/")
    result: dict[str, object] = {"trials_counted": False}
    with httpx.Client(trust_env=False, timeout=1.0) as client:
        get = client.get(provider_url + "/v1/effects?resource_ref=case/phase-d-one", headers=headers)
        body = {
            "trial_id": trial_id,
            "operation_id": "isolation-probe-operation",
            "actor": "agent-a",
            "provider": "phase-d-mock-shipment",
            "resource_ref": "case/phase-d-one",
            "action": "pack",
            "payload": {"case_ref": "case/phase-d-one", "quantity": 1},
            "authority_id": "isolation-probe-grant",
            "source_revision": "v1",
            "effect_key": "isolation-probe-effect",
        }
        post = client.post(provider_url + "/v1/effects", json=body, headers=headers)
        proxy = client.get(proxy_url + "/v1/effects?resource_ref=case/phase-d-one", headers=headers)
    result["direct_provider_get_status"] = get.status_code
    result["direct_provider_post_status"] = post.status_code
    result["unauthenticated_proxy_status"] = proxy.status_code
    result["provider_ledger_read_denied"] = _read_denied(os.environ["PHASED_PROVIDER_LEDGER_PATH"])
    result["proxy_ledger_read_denied"] = _read_denied(os.environ["PHASED_PROXY_LEDGER_PATH"])
    result["provider_process_env_read_denied"] = _read_denied(
        f"/proc/{os.environ['PHASED_PROVIDER_PID']}/environ",
    )
    result["proxy_process_env_read_denied"] = _read_denied(
        f"/proc/{os.environ['PHASED_PROXY_PID']}/environ",
    )
    forbidden = ("GITHUB_TOKEN", "GH_TOKEN", "GITHUB_PAT", "OPENAI_API_KEY", "ANTHROPIC_API_KEY")
    result["forbidden_credentials_absent"] = not any(os.environ.get(name) for name in forbidden)
    result["denied"] = all((
        result["direct_provider_get_status"] == 401,
        result["direct_provider_post_status"] == 401,
        result["unauthenticated_proxy_status"] == 401,
        result["provider_ledger_read_denied"] is True,
        result["proxy_ledger_read_denied"] is True,
        result["provider_process_env_read_denied"] is True,
        result["proxy_process_env_read_denied"] is True,
        result["forbidden_credentials_absent"] is True,
    ))
    print("PHASED_ISOLATION=" + json.dumps(result, sort_keys=True, separators=(",", ":")), flush=True)
    return 0 if result["denied"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
