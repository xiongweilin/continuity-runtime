# Architecture / scope

## Core invariants

1. Cross-boundary continuity is **selective**, not a global world database. Link IDs, owners, purposes, basis references, revisions and dependencies describe only necessary relationships.
2. Domain payload ownership stays outside the runtime. An actor observation records provenance, not an authoritative statement about arbitrary remote reality. For effect dispatch, every declared `expected_versions` reference requires an independently configured trusted `VersionAuthority` callable; if absent or mismatched, dispatch is held. This check is separate from the actor's local report, and external conditional writes are still necessary for atomic remote version guarantees.
3. The stable immutable `operation_id` is bound to principal, provider, target, action, canonical payload, authority reference and declared version preconditions. All participants need explicit grant membership. An optional domain-defined `effect_key` is unique per (principal, provider, resource) to prevent two sessions inventing different IDs for the same known logical effect. This key is an *application decision*, not a semantic-duplication detector.
4. Only a trusted executor calls an effecting provider. The database transaction marks the operation **may_have_been_sent before any I/O**. Even a crash between reservation and the call is treated conservatively.
5. `unknown` means effect may or may not have happened. No automatic replay. Only trusted readback can establish effect presence or sufficiently strong historical absence. Readback reporting absence/unknown while a dispatch is still in flight must NOT overwrite its `may_have_been_sent` state, as the external write may occur after the readback.
6. Effect confirmation is **not** the goal outcome. A domain-owned independent verifier must separately determine satisfaction. Once a domain outcome is recorded, it cannot be silently overwritten by another concurrent verifier; a future revision/reopening protocol would have to be explicit.
7. Authority and optional BAA admission are different. An `admit` decision cannot mint business authority. Recheck current grant and relevant preconditions at dispatch. PostgreSQL locks the grant and locally observed source versions across the final dispatch reservation, serializing local revocations and local updates. This does not prevent external source changes after readback, or cancel a reservation that already committed.
8. The workflow engine and Agent session are external. DBOS is optional, not a required Runtime dependency.

## Three layers

- **State expression**: selectively persisted links/observations/operations; can evolve with owners and domain schema.
- **Continuity and constrained consistency**: authority, versions, scoped effects, identity, recovery, audit.
- **Effectiveness**: external models, agents, workflows, controllers and verifiers; not wholly implemented by the Runtime.

## Trust and production boundaries

The administrator may configure `required_version_refs` on each grant. Agents cannot suppress those checks by submitting an empty `expected_versions` dictionary; guarded operations require independent trusted version readers and final local revalidation. Other domain-specific preconditions still need enforcement at the target executor.

The HTTP bearer token only authenticates a caller identity. For effects, a separately granted authorization must cover principal, actors, provider, action and target; the grant can be revoked. Production requires upstream TLS, enterprise auth, secrets rotation, proper tenancy and an independent access-control review. Grants are a minimal example, not a replacement for sophisticated IAM.

Each grant may name an `admission_profile`, which is persisted by the authority issuer and selected on the trusted Runtime side. A missing nonbaseline checker fails closed; admission never silently falls back to the baseline. Profile names are not cryptographic attestation of checker implementation and do not replace code review or deployment control.

A **bounded BAA-inspired canary stage policy** is implemented in `canary_admission.py`: trusted telemetry source, stage sequence, freshness, rollback checks, request coverage and error/latency guardrails. This is **not the complete BAA-Protocol**, a global risk budget or a formal proof. The baseline has no safety guarantee beyond the stated engineering constraints. **Never market it as BAA enforcement without an actual, verified BAA implementation and full mediation.**

The external readback adapter must be trusted and correctly implement evidence and historical effect semantics. An empty current snapshot does not establish historical non-effect. SQL row locks protect Runtime coordination only, not arbitrary remote requests, network packets or old workers.

## Durability

PostgreSQL is the intended concurrency backend. SQLite is a unit-test/example backend and has different locking semantics. Local state commits include an outbox record with each boundary event. Event sequence IDs are assigned under a database row lock held until producer commit, preventing a cursor from skipping a lower ID that commits later. This deliberately serializes the small cross-boundary event stream; high-volume deployments should benchmark or partition it rather than assuming unlimited throughput. Indexed link-dependency rows support targeted invalidation events without scanning every relation. Source revision changes also persistently deactivate affected links and increment their revision. A link owner must explicitly requalify them against the new source version. Grant revocation is also visible to participants of registered operations affected by that grant, without disclosing other grants. The API exposes an admin event cursor and actor-filtered polling with durable, revision-checked consumer offsets. One offered event batch is frozen until an exact-boundary acknowledgement, so changing poll limits cannot silently extend a pending batch. This is a minimal **at-least-once** pull-based handoff, not a distributed broker: consumers must process idempotently and acknowledge only after persistence. External brokers and push delivery are intentionally out of scope until an integration requires them.

Schema migration uses Alembic with an initial versioned baseline. **Online/zero-downtime migration is not guaranteed**; destructive downgrade is local-experiment-only. Credential isolation, workload-specific verification and deployment SLOs remain prerequisites before production.
