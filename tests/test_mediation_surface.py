"""Revision-pinned HTTP boundary inventory, inspired by BAA mediation studies.

A new API route must receive explicit review before it can mutate runtime state.
This checks HTTP, NOT that leaked provider tokens cannot bypass the runtime.
"""
from __future__ import annotations

from fastapi.routing import APIRoute
from continuity.api import Caller, create_app

EXPECTED_V1_SURFACE = {
    ("POST", "/v1/grants"): "issue",
    ("POST", "/v1/grants/{grant_id}/revoke"): "revoke",
    ("POST", "/v1/observations"): "observation",
    ("POST", "/v1/links"): "link",
    ("GET", "/v1/links/{link_id}"): "read_link",
    ("POST", "/v1/operations"): "submit",
    ("GET", "/v1/operations/{operation_id}"): "inspect",
    ("POST", "/v1/operations/{operation_id}/dispatch"): "dispatch",
    ("POST", "/v1/operations/{operation_id}/reconcile"): "reconcile",
    ("POST", "/v1/operations/{operation_id}/verify-outcome"): "verify",
    ("GET", "/v1/admin/recovery-queue"): "recovery_queue",
    ("GET", "/v1/events"): "stream",
    ("GET", "/v1/my-events"): "my_events",
    ("POST", "/v1/subscriptions"): "subscribe",
    ("GET", "/v1/subscriptions/{subscription_id}"): "poll",
    ("POST", "/v1/subscriptions/{subscription_id}/ack"): "ack",
}


def _has_principal_check(dependency) -> bool:
    if getattr(dependency.call, "__name__", None) == "principal":
        return True
    return any(_has_principal_check(child) for child in dependency.dependencies)


def test_all_runtime_http_routes_are_reviewed_and_authenticated(setup):
    runtime, _ = setup
    app = create_app(runtime, tokens={"reviewer": Caller("reviewer")})
    routes = [
        route for route in app.routes
        if isinstance(route, APIRoute) and route.path.startswith("/v1/")
    ]
    discovered = {
        (method, route.path): route.endpoint.__name__
        for route in routes for method in route.methods
    }
    assert discovered == EXPECTED_V1_SURFACE, (
        "new or modified runtime route requires explicit mediation review"
    )
    for route in routes:
        assert any(_has_principal_check(dep) for dep in route.dependant.dependencies), (
            "unmediated route: " + route.path
        )


def test_effect_routes_delegate_to_bounded_runtime_transition_methods(setup):
    import inspect

    app = create_app(setup[0], tokens={"reviewer": Caller("reviewer")})
    calls = {
        "/v1/operations/{operation_id}/dispatch": "runtime.dispatch(",
        "/v1/operations/{operation_id}/reconcile": "runtime.reconcile(",
        "/v1/operations/{operation_id}/verify-outcome": "runtime.verify_outcome(",
    }
    for route in app.routes:
        if isinstance(route, APIRoute) and route.path in calls:
            assert calls[route.path] in inspect.getsource(route.endpoint)
