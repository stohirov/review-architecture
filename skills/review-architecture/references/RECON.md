# Recon: profile a codebase

Build the **profile**: a compact picture of the system that every lens agent receives. Run recon yourself rather than in a subagent, because its output steers everything after it. Read build files, configuration and entry points; skim only enough business code to know what each deployable does.

## Signals

| Question | Where to look |
|---|---|
| Stack | `pom.xml`, `build.gradle*`, `package.json`, `go.mod`, `pyproject.toml`, `*.csproj`; framework versions |
| Deployables | each build module with a `main` / `@SpringBootApplication` / server entry point; `Dockerfile`s; services in `docker-compose*.yml`; k8s `Deployment`s, Helm charts |
| Sync comms | `RestTemplate`, `WebClient`, `RestClient`, `@FeignClient`, gRPC stubs, `HttpClient`, `fetch`/`axios`; base URLs in config pointing at other deployables |
| Async comms | `KafkaTemplate`, `@KafkaListener`, `RabbitTemplate`, `@RabbitListener`, `@JmsListener`, SQS/SNS/PubSub clients, Spring Cloud Stream bindings |
| Datastores | `spring.datasource.*`, JPA entities, Flyway/Liquibase folders, Redis/Mongo/Elasticsearch clients; the **database URL per deployable** |
| Scheduling and background work | `@Scheduled`, `@Async`, Quartz, ShedLock, executors, cron in manifests |
| Runtime and ops | actuator/health endpoints, replica counts, HPA, gateway or ingress config, tracing and metrics libraries |
| Stated intent | README, `docs/`, ADRs: target scale, SLAs, planned splits |

## Classify the architecture

- **monolith**: one deployable, with business logic not separated into modules.
- **modular-monolith**: one deployable with enforced module boundaries (separate packages with their own APIs, Spring Modulith, ArchUnit rules, separate schemas).
- **microservices**: several independently deployed services that own business capabilities and talk over the network. Several deployables sharing one database is still microservices here; report the sharing as a service-boundaries finding.
- **serverless**: functions as the main unit of deployment.
- **unknown**: say what is missing.

## Profile format

Keep it to 40 lines or fewer. It is pasted into every lens brief, and its fields map one to one onto `profile` in `findings-schema.json` (the JSON uses snake_case keys and arrays for the list fields).

```
architecture: <class>, <one-line reason>
summary: <what the system does, 1–2 sentences>
stack: <languages, frameworks, versions>
deployables:
  - <name> (<path>): <responsibility>; db=<datastore or none>; calls=<sync targets>; publishes=<topics>; consumes=<topics>
datastores: <engine and which deployables use each; flag any shared one>
messaging: <broker, topics or queues, or none>
background work: <schedulers, async executors, and where>
runtime: <replicas/scaling if declared, gateway, health checks, tracing>
stated intent: <scale targets, SLAs, plans, or unknown>
assumptions: <every decision you made in place of asking, or none>
derived load: <design mode, or when stated intent gives numbers: peak rates per flow you computed>
key flows: <1–3 business flows that cross a deployable or datastore boundary, as A -> B -> C>
```

**Key flows** matter most for microservices: they are where distributed-transaction, saga and resilience problems live. Trace at least the main write flow (for example, place order) end to end.
