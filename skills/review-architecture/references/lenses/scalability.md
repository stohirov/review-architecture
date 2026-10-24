# Lens: scalability

Can this system run as N identical instances behind a load balancer, and does its work per request stay bounded as data and traffic grow? Find what pins state to one instance, what does every unit of work N times when scaled out, and what grows with data instead of with the request.

Grade against the profile's stated intent. A single-instance internal tool with no growth plan does not need horizontal scaling. Say so, and cap these rules at low.

**Pattern vocabulary**: `externalize-session`, `stateless-service`, `object-storage`, `distributed-cache`, `cache-invalidation`, `distributed-lock`, `leader-election`, `async-offload`, `work-queue`, `pagination`, `read-replica`, `cqrs-read-model`, `backpressure`, `rate-limiting`, `horizontal-scaling`, `partitioning`, `cdn`.

---

### sc.in-memory-state · User or business state held in instance memory

- **Signals**:
  - `HttpSession` attributes holding carts or user data.
  - A `static` or singleton `Map`/`List` holding per-user or business state (`ConcurrentHashMap` carts, rate-limit counters, websocket subscriber registries, in-memory job status).
  - `server.servlet.session` with no `spring-session` store.
- **Why it fails**: the second instance does not see the first instance's state. Users lose carts or logins when the load balancer switches them, and a restart wipes everything. Sticky sessions hide this until a deploy or crash.
- **Fix**: `externalize-session` (Spring Session with Redis or JDBC) or `stateless-service` (state in the DB, or a token). Shared counters go in Redis.
- **Recommend when**: the service runs, or must run, on more than one instance. Critical when the profile shows several replicas or autoscaling.
- **Mitigations**: `spring-session-data-redis` or `spring-session-jdbc` on the classpath and configured.

### sc.local-filesystem · Files written to the local disk

- **Signals**: `new File(...)`, `Files.write`, `FileOutputStream` or `MultipartFile.transferTo` into a local directory (`/tmp`, `uploads/`, `user.home`) that is later served or read back by another request.
- **Why it fails**: other instances cannot see the file, and containers lose it on restart or reschedule.
- **Fix**: `object-storage` (S3, GCS, MinIO), storing only the key in the DB. Serve through pre-signed URLs or a `cdn`.
- **Recommend when**: the file must outlive the request or be read by another request. Scratch files deleted in the same request are clear.

### sc.local-cache-coherence · In-process cache of shared mutable data

- **Signals**: Caffeine, Guava cache, `@Cacheable` with a simple or `ConcurrentMapCacheManager`, or a hand-rolled `Map` cache of data that other instances or services can change (prices, stock, user profiles), with no TTL or invalidation.
- **Why it fails**: each instance serves its own stale copy. Updates on instance A never evict instance B's entry, so users see different prices on each refresh.
- **Fix**: a short TTL if staleness is acceptable; `distributed-cache` (Redis) with `cache-invalidation` on write; or invalidation events.
- **Recommend when**: the data changes and correctness or user-visible consistency matters. Immutable or reference data with a TTL is clear.

### sc.scheduler-every-instance · Scheduled job runs on every instance

- **Signals**: `@Scheduled`, Quartz in RAM mode, or a `ScheduledExecutorService` doing business work (sending invoices, expiring reservations, settlements, cleanup that sends notifications) with no ShedLock, `@SchedulerLock`, DB-backed Quartz cluster, or leader election.
- **Why it fails**: N instances run the job N times, which means duplicate emails, double charges or racing updates. Scaling out multiplies the bug.
- **Fix**: `distributed-lock` (ShedLock with a JDBC or Redis provider), `leader-election`, a clustered Quartz JDBC job store, or move the job to a single external scheduler (k8s `CronJob`).
- **Recommend when**: the job has side effects that must happen once. Idempotent per-instance housekeeping (clearing a local cache) is clear.
- **Mitigations**: `@SchedulerLock`, `LockProvider` beans, `spring.quartz.job-store-type=jdbc` with `isClustered=true`.

### sc.sync-heavy-work · Slow or heavy work done in the request thread

- **Signals**: a controller path that sends email or SMS through SMTP/SDK, generates PDFs or reports, processes images, calls slow third parties, or loops over large datasets before responding. Watch for `Thread.sleep`, `.block()` in WebFlux, and `CompletableFuture.get()`.
- **Why it fails**: latency is tied to the slowest dependency, the request threads (Tomcat's default is 200) fill up, and the whole service stops answering under load. A failure in a side task (email) fails the main operation.
- **Fix**: `async-offload` via a `work-queue` or broker (backed by the outbox if it must not be lost). Return 202 with a status resource for long jobs.
- **Recommend when**: the work is slow (hundreds of ms or more), unreliable, or not needed for the response. `@Async` alone loses work on crash and uses an unbounded pool by default; mention that when it is the mitigation.

### sc.unbounded-results · Endpoints or jobs that load unbounded result sets

- **Signals**: `findAll()` returning a `List` exposed by an API; queries with no `LIMIT` or `Pageable`; exports that load whole tables into memory; `stream()` over a full table without a cursor.
- **Why it fails**: response size and memory grow with the table. One request on a large table causes an OOM or a multi-second GC pause, and the database scans everything.
- **Fix**: `pagination` (keyset or cursor for large or deep pages, `Pageable` with a max size otherwise); a streaming export with a fetch size; and a cap on page size.
- **Recommend when**: the table grows with users or time. Small, bounded reference tables (countries, roles) are clear.

### sc.single-bottleneck · Single point of contention

- **Signals**:
  - A global `synchronized` method or lock around business work.
  - A single DB row updated by every request (a global counter, a sequence table).
  - A single-threaded executor serializing requests.
  - All reads and writes against one primary when the profile shows read-heavy traffic.
- **Why it fails**: throughput is capped by one lock or row no matter how many instances run, and `synchronized` does not even work across instances.
- **Fix**: remove the shared lock, or replace it with row-level or DB constraints; `partitioning` of hot counters; `read-replica` or `cqrs-read-model` for read-heavy paths.
- **Recommend when**: the contended path is on a hot flow.

### sc.no-backpressure · No limit on incoming or queued work

- **Signals**: unbounded `Executors.newCachedThreadPool()` or `LinkedBlockingQueue()`, `@Async` with the default executor (an unbounded queue), and no rate limiting on public or expensive endpoints.
- **Why it fails**: a burst queues unbounded work until memory runs out. Instead of shedding load, the service dies.
- **Fix**: bounded pools and queues with a rejection policy; `rate-limiting` at the gateway or with Bucket4j/Resilience4j; `backpressure` in consumers.
- **Recommend when**: the endpoint is public or expensive, or the executor handles request-driven work.
