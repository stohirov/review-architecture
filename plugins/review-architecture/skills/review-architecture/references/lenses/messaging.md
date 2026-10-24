# Lens: messaging

Does asynchronous communication deliver what the business needs, despite redelivery, poison messages, reordering, consumer lag and broker outages? Read producer config, consumer config and listener code together.

**Pattern vocabulary**: `dead-letter-queue`, `retry-topic`, `manual-ack-after-processing`, `partition-by-aggregate-key`, `idempotent-producer`, `schema-registry`, `event-versioning`, `consumer-lag-monitoring`, `event-carried-state-transfer`, `competing-consumers`.

---

### mq.commit-before-process · Offsets or acks committed before the work is done

- **Signals**:
  - Kafka `enable.auto.commit=true` (or `spring.kafka.consumer.enable-auto-commit: true`) with work that can fail.
  - `AckMode.RECORD`/`BATCH` combined with handing work to another thread (`@Async` or an executor inside the listener).
  - RabbitMQ `acknowledge-mode: none`/auto with async processing.
  - Manual `ack.acknowledge()` called before the business logic.
- **Why it fails**: the broker considers the message handled. A crash or exception afterwards loses it silently.
- **Fix**: `manual-ack-after-processing`: commit or ack only after the side effect commits, keep processing on the listener thread, and pair this with an `idempotent-consumer`.
- **Recommend when**: losing a message breaks a business invariant.

### mq.no-dlq · No dead-letter handling for poison messages

- **Signals**: no `DeadLetterPublishingRecoverer`, no `DefaultErrorHandler` with a recoverer, no `@RetryableTopic`, no RabbitMQ `x-dead-letter-exchange`; listeners that throw on bad input.
- **Why it fails**: with Kafka, a message that always fails blocks its partition forever (infinite retry), or is skipped and lost depending on the error handler. With Rabbit, requeue loops burn CPU.
- **Fix**: `retry-topic` with backoff, then a `dead-letter-queue`, with alerting and a replay tool.
- **Recommend when**: always for business consumers.

### mq.ordering-assumption · Ordering assumed but not guaranteed

- **Signals**:
  - Events for the same aggregate (order created, then paid, then cancelled) produced with a null or random key.
  - Several partitions with consumers that assume order.
  - `concurrency > 1` on a listener for an ordered stream.
  - RabbitMQ competing consumers on a queue where order matters.
- **Why it fails**: "cancelled" is processed before "created", so state machines break or reach a wrong final state.
- **Fix**: `partition-by-aggregate-key` (key = the aggregate id), and versioned or state-guarded handlers that reject out-of-order transitions.
- **Recommend when**: handlers apply transitions that depend on the previous state.

### mq.schema-evolution · Event contracts with no versioning

- **Signals**: events serialized from internal classes with Java serialization or default type headers (`spring.json.add.type.headers` with internal class names); no schema registry or version field; consumers sharing producer classes through a library.
- **Why it fails**: renaming a field or class in the producer breaks consumers at runtime, and old messages left in the topic cannot be read after a deploy.
- **Fix**: `event-versioning` (an explicit version field with tolerant readers) or a `schema-registry` (Avro/Protobuf/JSON Schema) with compatibility checks. Map to a public event type rather than serializing entities.
- **Recommend when**: producer and consumer deploy independently.

### mq.request-reply-over-broker · Synchronous behaviour tunnelled through a broker

- **Signals**: `ReplyingKafkaTemplate`, `RabbitTemplate.convertSendAndReceive`, or producers that block waiting for a reply event on the request path.
- **Why it fails**: it gets the latency and coupling of a sync call plus the operational complexity of a broker, and timeouts become ambiguous (did the work happen?).
- **Fix**: plain HTTP or gRPC with `timeouts` when you need an answer now; otherwise a truly async flow with a status resource.
- **Recommend when**: used on a user-facing request path.

### mq.no-lag-visibility · Consumer lag and DLQ volume unobserved

- **Signals**: no Micrometer Kafka metrics, no lag exporter, no DLQ alerts in config or manifests.
- **Why it fails**: consumers fall hours behind, or poison messages pile up, and nobody notices until users do.
- **Fix**: `consumer-lag-monitoring` with alerts on lag and DLQ depth.
- **Recommend when**: event-driven flows are business-critical. Usually medium or low.
