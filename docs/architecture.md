# Architecture / scope

## Core invariants

1. Cross-boundary continuity is **selective**, not a global world database. Link IDs, owners, purposes, basis references, revisions and dependencies describe only necessary relationships.
2. Domain payload ownership stays outside the runtime. An actor observation records provenance, not an authoritative statement about arbitrary remote reality. For effect dispatch, every declared `expected_versions` reference requires an independently configured trusted `VersionAuthority` callable; if absent or mismatched, dispatch is held. This check is separate from the actor's local report, and external conditional writes are still necessary for atomic remote version guarantees.
3. The stable immutable `operation_id` is bound to principal, provider, target, action, canonical payload, authority reference and declared version preconditions. All participants need explicit grant membership. An optional domain-defined `effect_key` is unique per (principal, provider, resource) to prevent two sessions inventing different IDs for the same known logical effect. This key is an *application decision*, not a semantic-duplication detector.
4. Only a trusted executor calls an effecting provider. The database transaction marks the operation **may_have_been_sent before any I/O**. Even a crash between reservation and the call is treated conservatively.
5. `unknown` means effect may or may not have happened. No automatic replay. Only trusted readback can establish effect presence or sufficiently strong historical absence.
6. Effect confirmation is **not** the goal outcome. A domain-owned independent verifier must separately determine satisfaction.
7. Authority and optional BAA admission are different. An `admit` decision cannot mint business authority. Recheck current grant and relevant preconditions at dispatch.
8. The workflow engine and Agent session are external. DBOS is optional, not a required Runtime dependency.

## Three layers

- **State expression**: selectively persisted links/observations/operations; can evolve with owners and domain schema.
- **Continuity and constrained consistency**: authority, versions, scoped effects, identity, recovery, audit.
- **Effectiveness**: external models, agents, workflows, controllers and verifiers; not wholly implemented by the Runtime.

## Trust and production boundaries

The HTTP bearer token only authenticates a caller identity. For effects, a separately granted authorization must cover principal, actors, provider, action and target; the grant can be revoked. Production requires upstream TLS, enterprise auth, secrets rotation, proper tenancy and an independent access-control review. Grants are a minimal example, not a replacement for sophisticated IAM.

The current `AdmissionPolicy` may be replaced by a domain-specific BAA policy, but the baseline has no safety guarantee beyond the stated engineering constraints. **Never market it as BAA enforcement without an actual, verified BAA implementation and full mediation.**

The external readback adapter must be trusted and correctly implement evidence and historical effect semantics. An empty current snapshot does not establish historical non-effect. SQL row locks protect Runtime coordination only, not arbitrary remote requests, network packets or old workers.

## Durability

PostgreSQL is the intended concurrency backend. SQLite is a unit-test/example backend and has different locking semantics. Local state commits include an outbox record with each boundary event. Indexed link-dependency rows support targeted invalidation events without scanning every relation. Source revision changes also persistently deactivate affected links and increment their revision. A link owner must explicitly requalify them against the new source version. The API currently exposes a read-only admin event cursor; it is **not yet** a durable multi-consumer subscription manager. Event dispatch/brokers can be added when a real integration needs them.

Schema migration uses Alembic with an initial versioned baseline. **Online/zero-downtime migration is not guaranteed**; destructive downgrade is local-experiment-only. Credential isolation, workload-specific verification and deployment SLOs remain prerequisites before production.
