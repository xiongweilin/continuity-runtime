# Security status

**Engineering alpha — not production approved.**

Please do not connect production write credentials or expose the example bearer-token
registry over the public internet. Domain-specific adapters must enforce resource
scope and must not grant proposing Agents direct provider credentials.

The Effect Coordinator fences a single recorded operation ID inside PostgreSQL,
not arbitrary duplicate business intents or old network packets. A lost response
is UNKNOWN, not proof of no effect. Actual remote idempotency, conditional writes,
credential isolation, independent evidence, and historical effect recovery are
deployment responsibilities.

The BaselinePolicy is **not** BAA certification. A proposed action, a policy
admission, a business authorization, an external effect and a verified domain
outcome are non-interchangeable. Full BAA guarantee coverage requires separate
deployment-specific evaluation and enforcement.

This repository has not been independently security audited and has no published
production safety guarantee. When reporting vulnerabilities, avoid posting
credentials or sensitive test data in public issues.
