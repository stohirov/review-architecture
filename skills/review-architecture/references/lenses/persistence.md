# Lens: persistence

Does data access stay efficient and safe as data grows? Read entities, repositories, queries, migrations and datasource config.

Unbounded result sets belong to the scalability lens (`sc.unbounded-results`). Report them here only if that lens is not running.

**Pattern vocabulary**: `fetch-join`, `entity-graph`, `batch-fetching`, `dto-projection`, `add-index`, `disable-open-in-view`, `connection-pool-sizing`, `versioned-migrations`, `expand-contract-migration`, `bulk-operations`, `read-replica`, `data-retention`.

---

### ps.n-plus-one · N+1 queries over associations

- **Signals**: loops over entities that touch a lazy `@OneToMany`/`@ManyToOne` (`order.getItems()` inside a `for` over orders); `FetchType.EAGER` on collections; mapping entities to DTOs in a stream that triggers lazy loads; no `JOIN FETCH`/`@EntityGraph` on list queries.
- **Why it fails**: one page of 100 rows becomes 101+ queries. Latency and DB load grow with page size and data.
- **Fix**: `fetch-join` or `entity-graph` for the query that needs it, `batch-fetching` (`@BatchSize`, `hibernate.default_batch_fetch_size`), or `dto-projection`.
- **Recommend when**: on list or report paths, or in loops over growing collections.

### ps.missing-index · Queries filter or join on unindexed columns

- **Signals**:
  - Derived queries (`findByEmail`, `findByStatusAndCreatedAtBefore`) or `@Query`s whose `WHERE`/`JOIN`/`ORDER BY` columns have no index in the migrations or `@Table(indexes=…)`.
  - Foreign keys without indexes (Postgres does not create them automatically).
  - Uniqueness enforced only in Java (`existsByEmail` then save) with no unique constraint.
- **Why it fails**: every query becomes a full table scan once the table grows. Check-then-insert races create duplicates.
- **Fix**: `add-index` (composite indexes in query-column order) and unique constraints in a migration.
- **Recommend when**: the table grows and the query is on a hot path. Note that `ddl-auto` hides the problem (`ps.schema-management`).

### ps.open-in-view · Open Session in View left enabled

- **Signals**: `spring.jpa.open-in-view` unset (it defaults to true, with a startup warning) or `true`, in a service that serves APIs.
- **Why it fails**: the DB connection is held for the whole request, including view rendering and remote calls. Lazy loads fire from serializers, which hides N+1 and exhausts the pool under load.
- **Fix**: `disable-open-in-view`, and fetch what each use case needs explicitly.
- **Recommend when**: always for API services. Medium by default; high if remote calls happen in controllers.

### ps.schema-management · Schema not under versioned migrations

- **Signals**: `spring.jpa.hibernate.ddl-auto=update` or `create` outside tests; no Flyway or Liquibase; destructive migrations (dropping or renaming columns) shipped in the same release as the code change.
- **Why it fails**: `ddl-auto=update` never drops or renames, silently diverges between environments, can lock tables at startup, and gives no review or rollback. Rename-in-one-step breaks the instances still running the old version during a rolling deploy.
- **Fix**: `versioned-migrations` (Flyway or Liquibase) with `ddl-auto=validate`; `expand-contract-migration` for renames and drops.
- **Recommend when**: anything beyond a prototype.

### ps.pool-sizing · Connection pool mis-sized for the workload

- **Signals**:
  - Hikari `maximum-pool-size` far above what the DB allows across all replicas (replicas × pool > `max_connections`).
  - Default pool size with many request threads and long transactions.
  - No `connection-timeout` tuning on hot services.
- **Why it fails**: scaling out replicas exhausts DB connections, and new instances fail to start or queue forever. Too small a pool plus long transactions causes request pile-ups.
- **Fix**: `connection-pool-sizing` against DB limits and replica count; a pooler (PgBouncer); shorter transactions.
- **Recommend when**: replicas or autoscaling are declared, or long transactions exist.

### ps.unbounded-growth · Tables that grow forever

- **Signals**: event logs, audit, outbox or processed-message tables, sessions or notifications with no cleanup job, TTL, partitioning or archival.
- **Why it fails**: indexes and table scans slow down month by month, and an outbox relay that polls an unbounded table degrades.
- **Fix**: `data-retention` (scheduled purge or partition dropping) and time-based partitioning.
- **Recommend when**: the table receives a row per request or event.

### ps.row-by-row-writes · Bulk work done one row at a time

- **Signals**: `save()` in a loop over large collections; no `hibernate.jdbc.batch_size`; `IDENTITY` id generation (which disables JDBC batching) on bulk-inserted entities.
- **Why it fails**: imports and batch jobs make thousands of round trips and hold transactions open for a long time.
- **Fix**: `bulk-operations` (`saveAll` with batch size and `SEQUENCE` ids, JDBC batch or COPY for imports, chunked transactions).
- **Recommend when**: the loop handles hundreds of rows or more.
