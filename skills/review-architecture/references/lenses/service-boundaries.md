# Lens: service boundaries

Are the units of deployment, and the modules inside a monolith, split along business capabilities so that each can change, deploy and fail independently? Check who owns which data, how chatty the seams are, and what is shared.

Proportionality matters most here. **Do not recommend splitting a monolith into microservices** unless the profile shows the forces that justify it: several teams blocked on each other, parts with very different scaling or availability needs, or deploy contention. For a small team, recommend a **modular monolith** instead.

**Pattern vocabulary**: `database-per-service`, `api-composition`, `data-replication-via-events`, `merge-services`, `modular-monolith`, `bounded-context`, `anti-corruption-layer`, `consumer-driven-contracts`, `api-versioning`, `strangler-fig`, `backend-for-frontend`.

---

### sb.shared-database · Several services share one database or schema

- **Signals**:
  - Two or more deployables with the same `spring.datasource.url` (or schema).
  - Entities or tables mapped in more than one service.
  - A service querying or joining another service's tables.
  - Migrations for one service's tables living in another's folder.
- **Why it fails**:
  - Services are coupled through the schema: a column change in one breaks the other at runtime.
  - Teams can't deploy independently.
  - One service's heavy queries starve the other.
  - No service owns the invariants of the shared rows.
- **Fix**: `database-per-service` (separate schemas at minimum, with no cross-schema access), where each service exposes data through its API or events (`api-composition`, `data-replication-via-events`). If the services are so entangled that this is impractical, `merge-services`.
- **Recommend when**: the services are meant to be independently deployable. A single team running two processes against one DB on purpose (an API plus a worker for the same bounded context) is clear.

### sb.distributed-monolith · Services that must change and deploy together

- **Signals**:
  - Most key flows need sync calls across several services (see `rs.sync-call-chain`).
  - A shared `common`/`domain` library containing entities or business logic, imported by every service.
  - Services exchanging internal entities as DTOs.
  - Several services implementing one business rule.
  - Lockstep versioning in build files.
- **Why it fails**: you pay the costs of microservices (network failures, distributed data, ops overhead) without the benefits (independent change and deploy). Changing one feature means coordinated releases.
- **Fix**: redraw boundaries around `bounded-context`s. `merge-services` that always change together. Keep shared libraries to technical concerns only (logging, tracing), never domain models. Use `consumer-driven-contracts` at the seams that remain.
- **Recommend when**: there is evidence of co-change or shared domain code, not merely because there are several services.

### sb.chatty-interface · Fine-grained calls across a service boundary

- **Signals**: loops that call another service per item (N+1 over HTTP); a UI or service assembling one view from many small calls; getter-style endpoints (`/customers/{id}/email`).
- **Why it fails**: latency and failure probability scale with the number of calls, and load on the callee grows with the caller's data size.
- **Fix**: batch endpoints; coarse-grained APIs shaped around use cases; `data-replication-via-events` so the caller holds a local read model; `backend-for-frontend` for UI aggregation.
- **Recommend when**: the loop is on a hot path or unbounded.

### sb.leaky-boundary · Internal model exposed or bypassed

- **Signals**:
  - JPA entities returned straight from controllers or put on topics.
  - Another service's enum or entity class copied in.
  - Modules in a monolith reaching into another module's repositories or internal packages.
  - No anti-corruption mapping around a third-party API.
- **Why it fails**: internal refactors become breaking API changes. In a monolith, the module structure erodes until everything depends on everything.
- **Fix**: DTOs and versioned contracts (`api-versioning`); module APIs with internal packages hidden (Spring Modulith, ArchUnit, Java modules); an `anti-corruption-layer` for third parties.
- **Recommend when**: the model crosses a team or deploy boundary, or the monolith has several business modules.

### sb.premature-split · Microservices without the forces that justify them

- **Signals**: many services for a small team (more services than engineers), services with a single endpoint, or nearly all traffic needing every service, and the profile shows no independent scaling or deploy needs.
- **Why it fails**: the distributed-systems tax (consistency, latency, observability, deploy pipelines) with no payoff.
- **Fix**: `modular-monolith`; merge services along bounded contexts; keep the module seams so they can be split later.
- **Recommend when**: the profile's team and scale information supports it. Raise it as medium with medium confidence, because the reviewer rarely sees the whole organizational picture.
