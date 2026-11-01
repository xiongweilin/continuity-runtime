"""Optional DBOS bridge; its step calls the same Continuity API used by other agents.

Install pip install -e '.[dbos]' and provide DBOS_SYSTEM_DATABASE_URL,
CONTINUITY_API_BASE and CONTINUITY_AGENT_TOKEN. Not required by the core.
"""
from __future__ import annotations

import os

import httpx
from dbos import DBOS, DBOSConfig


@DBOS.step()
def continue_effect(operation_id: str) -> dict:
    url = os.environ["CONTINUITY_API_BASE"].rstrip("/")
    headers = {"Authorization": "Bearer " + os.environ["CONTINUITY_AGENT_TOKEN"]}
    with httpx.Client(base_url=url, headers=headers, timeout=30) as client:
        result = client.post(f"/v1/operations/{operation_id}/dispatch")
        result.raise_for_status()
        result = client.post(f"/v1/operations/{operation_id}/reconcile")
        result.raise_for_status()
        return result.json()


@DBOS.workflow()
def resume_handoff(operation_id: str) -> dict:
    return continue_effect(operation_id)


if __name__ == "__main__":
    config: DBOSConfig = {
        "name": "continuity-example",
        "application_version": "0.1.0",
        "system_database_url": os.environ["DBOS_SYSTEM_DATABASE_URL"],
    }
    DBOS(config=config)
    DBOS.launch()
    try:
        print(resume_handoff(os.environ.get("CONTINUITY_OPERATION_ID", "handoff-op")))
    finally:
        DBOS.destroy()
