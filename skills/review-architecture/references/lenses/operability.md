# Lens: operability

Can this system be deployed, scaled and debugged in production without heroics? Read deploy manifests, actuator and logging config, and how configuration and secrets reach the app.

Security is out of scope beyond architecture-level gaps (secrets in source, no auth boundary). Point to a dedicated security review for depth.

**Pattern vocabulary**: `health-probes`, `graceful-shutdown`, `externalized-config`, `secrets-manager`, `distributed-tracing`, `correlation-id`, `structured-logging`, `metrics-and-alerts`, `immutable-artifacts`, `rolling-deploy-compatibility`, `api-gateway`, `resource-requests-and-disruption-budget`, `startup-probe`.

---

### op.no-health-probes · No meaningful liveness or readiness checks

- **Signals**: no `spring-boot-starter-actuator`; no `livenessProbe`/`readinessProbe` in manifests; readiness that ignores critical dependencies, or liveness that checks the DB (which causes restart storms when the DB blips).
- **Why it fails**: the load balancer routes traffic to instances that are not ready or are stuck, and orchestrators cannot heal or roll out safely.
- **Fix**: `health-probes`: liveness checks only the process; readiness checks what the instance needs to serve (DB, required caches). Use the Spring Boot `management.endpoint.health.probes.enabled=true` groups.
- **Recommend when**: running on an orchestrator or behind a load balancer.

### op.no-graceful-shutdown · In-flight work dropped on deploy

- **Signals**: no `server.shutdown=graceful`; no `spring.lifecycle.timeout-per-shutdown-phase`; consumers and schedulers not stopped cleanly; `terminationGracePeriodSeconds` shorter than the longest request.
- **Why it fails**: every deploy or scale-in cuts requests and half-processed messages, which produces sporadic errors and partial writes.
- **Fix**: `graceful-shutdown` plus a readiness flip before the drain.
- **Recommend when**: rolling deploys or autoscaling are used.

### op.hardcoded-config · Environment-specific config or secrets in code

- **Signals**: URLs, hostnames, credentials or API keys hardcoded in Java or committed in `application.yml`/`.properties`; per-environment builds; other services' addresses as literals.
- **Why it fails**: you cannot promote one artifact through environments, rotating a secret needs a rebuild, and secrets leak through the repo.
- **Fix**: `externalized-config` (env vars, a config server, k8s ConfigMaps) and a `secrets-manager` (Vault, cloud secret stores, k8s Secrets); `immutable-artifacts`.
- **Recommend when**: always for secrets (high); medium for other environment-specific values.

### op.no-tracing · Requests cannot be followed across services

- **Signals**: no Micrometer Tracing or OpenTelemetry or Sleuth; no trace or correlation id propagated through HTTP headers and message headers; logs without a trace id.
- **Why it fails**: when a multi-service flow fails, nobody can tell where. Mean time to recovery is measured in hours of grepping.
- **Fix**: `distributed-tracing` with `correlation-id` propagation through HTTP and the broker, plus `structured-logging` that includes trace ids.
- **Recommend when**: more than one deployable takes part in a flow. For a monolith, a correlation id in logs is enough (low).

### op.no-metrics · No metrics or alerts on the signals that matter

- **Signals**: no Micrometer registry or Prometheus endpoint; no alerting config; no business-level metrics on key flows.
- **Why it fails**: degradation (latency, error rate, saturation, a stuck saga) is discovered by users.
- **Fix**: `metrics-and-alerts`: the RED metrics per endpoint and dependency, USE metrics for pools and queues, and alerts on SLO burn.
- **Recommend when**: production services. Usually medium.

### op.no-edge · No single, controlled entry point

- **Signals**: clients call several services directly; every service implements its own authentication, CORS and rate limiting; no gateway or ingress in manifests.
- **Why it fails**: cross-cutting policy drifts between services, clients are coupled to internal topology, and internal services are exposed.
- **Fix**: an `api-gateway` or ingress handling routing, auth and rate limits, with internal services kept off the public network.
- **Recommend when**: several services are reached by external clients.

### op.no-resource-policy · Pods without resources or disruption limits

- **Signals**: k8s Deployments with no CPU or memory `requests`/`limits`; an HPA on CPU utilization with no CPU request (it cannot compute utilization); no `PodDisruptionBudget`; no topology spread for replicated services; slow startup (migrations at boot) with no `startupProbe`.
- **Why it fails**: the scheduler overpacks nodes, the HPA never scales, node drains take every replica down at once, and slow starts crash-loop under liveness checks.
- **Fix**: `resource-requests-and-disruption-budget` (requests and limits, a PDB, spread constraints) and a `startup-probe`.
- **Recommend when**: running on k8s with more than one replica or an HPA.

### op.unsafe-rolling-deploy · Versions that cannot run side by side

- **Signals**: breaking API or event changes with no versioning; schema migrations that break the previous app version; startup jobs that assume a single instance.
- **Why it fails**: during a rolling deploy, old and new instances run together and break each other, which forces downtime deploys.
- **Fix**: `rolling-deploy-compatibility`: expand/contract migrations, tolerant readers, versioned APIs and events.
- **Recommend when**: zero-downtime deploys are expected.
