# Lens: data consistency

Can a crash, retry, timeout or concurrent request leave data wrong: an order without a payment, a charge without an order, stock counted twice? Trace transaction boundaries, and every point where one business operation touches more than one resource (two databases, a database and a broker, a database and a remote API).

**Pattern vocabulary**: `transactional-outbox`, `saga-orchestration`, `saga-choreography`, `compensating-action`, `idempotency-key`, `idempotent-consumer`, `optimistic-locking`, `pessimistic-locking`, `after-commit-hook`, `move-call-outside-transaction`, `listen-to-yourself`, `reservation-pattern`.

Each rule gives its signals, why it fails, the fix, when to recommend it, and the mitigations that clear it.

---

### dc.remote-call-in-tx · Remote call inside a database transaction

- **Signals**: a `@Transactional` method (or `TransactionTemplate`, or explicit `begin`/`commit`) whose call path reaches `RestTemplate`, `WebClient…block()`, `RestClient`, a `@FeignClient`, a gRPC stub, or an SDK for a third party (payment, email, SMS). Follow helpers across classes; the call is often two hops away.
- **Why it fails**:
  - The remote side effect cannot roll back. If the local transaction rolls back after the call succeeds, the remote system keeps the charge, reservation or email while the local row vanishes.
  - If the call succeeds but its response is lost (a timeout), the local side rolls back, and the retry repeats the side effect.
  - The DB connection is held for the call's full latency, so a slow dependency drains the connection pool and takes down unrelated endpoints.
- **Fix**:
  - Commit local state first, with a status such as `PENDING`.
  - Do the remote step after commit, through `transactional-outbox` or an `after-commit-hook` for non-critical calls.
  - Make the remote call idempotent with an `idempotency-key`.
  - For multi-step business flows, see `dc.no-saga`.
- **Recommend when**: the remote call has a side effect, or is slow or unreliable. A read-only lookup inside a transaction is medium at most (pool exhaustion), not a consistency bug.
- **Mitigations**:
  - The call is made in `@TransactionalEventListener(phase = AFTER_COMMIT)`.
  - The method is `propagation = NOT_SUPPORTED`.
  - An outbox row is written instead of the call.

### dc.dual-write · Database write plus broker publish without an outbox

- **Signals**: `repository.save(…)` and `kafkaTemplate.send` / `rabbitTemplate.convertAndSend` / `streamBridge.send` / SNS publish in the same method or flow, with no outbox table.
- **Why it fails**:
  - Publish inside the transaction: the event goes out, then the transaction rolls back, and consumers act on data that does not exist.
  - Publish after commit: a crash between commit and publish loses the event forever, and downstream services never learn about the change.
  - Kafka transactions do not span the database. `KafkaTransactionManager` plus `JpaTransactionManager` (or a chained manager) is best-effort, not atomic.
- **Fix**:
  - `transactional-outbox`: insert the event into an `outbox` table in the same local transaction, and publish it with a relay (polling publisher or CDC such as Debezium) that marks rows sent.
  - Consumers must then be idempotent (`dc.non-idempotent-consumer`).
  - Alternative: `listen-to-yourself`, where the service consumes its own event to update its DB.
- **Recommend when**: another service or process depends on the event to stay correct. For fire-and-forget analytics, medium at most.
- **Mitigations**:
  - An outbox entity, table or migration with a relay or scheduler that reads it.
  - A Debezium or CDC connector config.
  - Spring Modulith's event publication registry.

### dc.no-saga · Multi-service business transaction with no saga or compensation

- **Signals**:
  - A key flow that changes state in two or more services (order → payment → inventory → shipping) as a chain of sync calls or events.
  - No step reverses an earlier step when a later one fails: no `refund`, `release`, `cancel`, no compensation handler, no saga state machine or status column driving the flow.
  - Look for the failure branch. Does a `catch` block, or a failure event, undo earlier steps?
- **Why it fails**: when step 3 fails, steps 1–2 stay applied. The customer is charged with no stock reserved, or stock stays reserved for a cancelled order. Nothing records how far the flow got, so recovery is manual.
- **Fix**:
  - `saga-orchestration`: an orchestrator persists saga state and issues commands plus compensations. Prefer it for flows of three or more steps, or with branching.
  - `saga-choreography`: services react to each other's events, each with a compensating action. Fine for two or three steps.
  - Both need the outbox for reliable steps and `idempotent-consumer` for safe retries.
  - Use `reservation-pattern` (reserve, then confirm or expire) for stock and seats.
- **Recommend when**: the flow spans services with separate databases. **Do not** recommend a saga when every step writes to one database; a local transaction is correct there. Two-phase commit/XA is not the fix (`dc.distributed-2pc`).
- **Mitigations**: compensation endpoints wired to failure paths; a saga or process-manager entity with states; a Temporal/Camunda/Axon saga.

### dc.non-idempotent-consumer · Message handler or retried endpoint is not idempotent

- **Signals**:
  - A `@KafkaListener` or `@RabbitListener` handler that inserts, increments or charges without checking a processed-message id or a natural unique key.
  - A POST endpoint that clients or gateways retry, with no idempotency key.
  - `stock = stock - qty` style updates driven by events.
- **Why it fails**: brokers deliver at least once, so redelivery after a rebalance, a crash before the offset commit, or a retry runs the side effect twice: a double charge, double stock decrement or duplicate order.
- **Fix**:
  - `idempotent-consumer`: store processed message ids in a table with a unique constraint, in the same transaction as the effect.
  - Or make the operation naturally idempotent: an upsert keyed by business id, or a state-transition check (`UPDATE … WHERE status = 'PENDING'`).
  - For HTTP: an `idempotency-key` header stored with the result.
- **Recommend when**: the handler has a non-idempotent side effect. Pure upserts or overwrites are clear.
- **Mitigations**: a processed-events table or unique constraint on the event id; a status guard in the `WHERE` clause; Kafka exactly-once used end to end with no external side effects (rare).

### dc.distributed-2pc · XA or two-phase commit across services or resources

- **Signals**: Atomikos, Narayana, Bitronix, `JtaTransactionManager`, XA datasources, `ChainedTransactionManager` across a DB and a broker.
- **Why it fails**:
  - It couples availability: every participant must be up.
  - It holds locks across the network, and leaves in-doubt transactions when the coordinator dies.
  - `ChainedTransactionManager` is not atomic at all; it is deprecated best-effort ordering.
- **Fix**: `transactional-outbox` plus `saga-orchestration`, or a single-resource design.
- **Recommend when**: always in microservices. Within one monolith with one DB and one broker, medium (prefer the outbox).

### dc.lost-update · Read-modify-write without concurrency control

- **Signals**:
  - Load an entity, change a counter or balance or stock field in Java, then save, with no `@Version`, no `SELECT … FOR UPDATE` (`@Lock(PESSIMISTIC_WRITE)`), and no atomic `UPDATE … SET x = x - :n WHERE x >= :n`.
  - A check-then-act on stock or balance ("if available ≥ qty then save").
- **Why it fails**: two concurrent requests read the same value and both write, so one update is lost. This is overselling, or negative balances.
- **Fix**: `optimistic-locking` (`@Version` and retry) for low contention; an atomic conditional `UPDATE` or `pessimistic-locking` for hot rows.
- **Recommend when**: the field is shared across concurrent requests (stock, balance, seat count). Per-user private data rarely needs it.

### dc.tx-boundary-misuse · Transactions that do not do what the code assumes

- **Signals**:
  - A `@Transactional` method called from the same class (self-invocation bypasses the proxy).
  - `@Transactional` on a private method.
  - Checked exceptions that do not trigger rollback (no `rollbackFor`).
  - A `try/catch` that swallows an exception inside a transaction, and so commits partial work.
  - A long transaction wrapping a batch loop or file processing.
- **Why it fails**: writes the author believes are atomic are not, or a transaction holds locks and connections for minutes.
- **Fix**: move the transactional method to another bean or use `TransactionTemplate`; use `rollbackFor`; keep transactions short and batch in chunks.
- **Recommend when**: the non-atomic writes must succeed or fail together.
