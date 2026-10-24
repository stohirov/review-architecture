# Lens: resilience

When a dependency is slow, down or flapping, does the failure stay contained, or does it spread? Check every outbound call (to other services, third parties, brokers and caches) for bounded waiting, sensible retries and isolation.

**Pattern vocabulary**: `timeouts`, `retry-with-backoff`, `circuit-breaker`, `bulkhead`, `fallback`, `async-decoupling`, `graceful-degradation`, `health-based-routing`, `load-shedding`, `idempotency-key`.

---

### rs.no-timeouts · Outbound calls without timeouts

- **Signals**:
  - `new RestTemplate()` or a `RestTemplateBuilder` with no `setConnectTimeout`/`setReadTimeout`.
  - A `WebClient` with no `responseTimeout` or `.timeout(...)`.
  - Feign with no `connectTimeout`/`readTimeout` in config.
  - JDK `HttpClient` with no `.timeout`; JDBC with no query timeout on hot paths.
  - Check both code and `application.yml`.
- **Why it fails**: the defaults are infinite or very long. A hung dependency holds the caller's threads (and DB connections, if inside a transaction) until the caller's own pool is exhausted, so one slow service takes down everything upstream.
- **Fix**: `timeouts` on every client: connect around 1 s; read set from the dependency's p99 with headroom. Set an overall deadline for the request.
- **Recommend when**: always, for network calls on request paths. High when the call sits on a key flow.
- **Mitigations**: timeouts set in a shared client config bean or `application.yml` (`spring.cloud.openfeign.client.config.default.*`, `resilience4j.timelimiter`).

### rs.no-circuit-breaker · Repeated calls into a failing dependency

- **Signals**: no Resilience4j, Spring Cloud CircuitBreaker, or service-mesh outlier detection around calls to other services or third parties on hot paths.
- **Why it fails**: while a dependency is down, every request still waits for the full timeout, which wastes threads and keeps load on a struggling dependency, slowing its recovery.
- **Fix**: `circuit-breaker` around each dependency, with a `fallback` or a fast failure, and `graceful-degradation` (for example, show the product without recommendations).
- **Recommend when**: the service makes sync calls to dependencies that can fail independently and the call volume is meaningful. For one low-volume internal call, low.
- **Mitigations**: `@CircuitBreaker`, `CircuitBreakerFactory`, mesh config (Istio `outlierDetection`) in the deploy manifests.

### rs.unsafe-retries · Retries without backoff, limits or idempotency

- **Signals**: `@Retryable`, Resilience4j `retry`, Feign `Retryer`, or hand-written loops that retry immediately or without a cap. Retries of non-idempotent POSTs (payments, orders) with no idempotency key. Retries stacked at several layers (client, gateway, mesh).
- **Why it fails**: retry storms multiply load on a struggling dependency (three layers of three retries is 27×). Retrying a non-idempotent call duplicates the side effect when the first attempt actually succeeded.
- **Fix**: `retry-with-backoff` with jitter and a small cap, at one layer only; retry only idempotent operations, or add an `idempotency-key`.
- **Recommend when**: retries exist on calls with side effects, or with no backoff.

### rs.sync-call-chain · Long synchronous call chains on the request path

- **Signals**: a user request path that blocks on two or more hops in sequence (A calls B, and B calls C or a third party, before A can respond), visible in the profile's key flows. A→B→third-party counts. Report it on its own even when `rs.no-timeouts` also fires: timeouts bound the wait, but this rule is about the multiplied availability and the coupling, and its fix is different.
- **Why it fails**: availability multiplies (three services at 99.9% give about 99.7%), latency adds up, and any link's failure fails the whole request. This is the distributed-monolith shape.
- **Fix**: `async-decoupling` (accept the request, then progress the flow through events or a saga), or cache or replicate the data A needs, so the chain shortens to one hop.
- **Recommend when**: the chain is three or more services deep, or ends in a third party the user must wait for.

### rs.no-bulkhead · Shared resources for unrelated dependencies

- **Signals**: one shared HTTP connection pool or thread pool for every outbound dependency; a single `@Async` executor for everything; one DB pool for both OLTP and reporting queries.
- **Why it fails**: one slow dependency fills the shared pool, and calls to healthy dependencies queue behind it.
- **Fix**: `bulkhead`: separate bounded pools per dependency, or Resilience4j `@Bulkhead`.
- **Recommend when**: several dependencies with different reliability share a pool on a hot path. Otherwise medium or low.

### rs.critical-path-optional-dependency · Non-essential dependency can fail the core flow

- **Signals**: a core flow (checkout, booking, login) that calls notification, analytics, recommendation, audit or loyalty services synchronously and lets their exception propagate.
- **Why it fails**: an outage of a nice-to-have feature becomes an outage of the core business.
- **Fix**: `async-decoupling` (publish an event) or `fallback` with `graceful-degradation`, so failures are caught, logged and retried out of band.
- **Recommend when**: the dependency is not needed to answer the request correctly.
