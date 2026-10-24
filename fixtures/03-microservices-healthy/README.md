# Order platform (v2)

Three Spring Boot services, each owning its own database:

| Service | Team | Owns |
|---|---|---|
| `order-service` | Checkout team | Orders and the order saga (orchestrator) |
| `inventory-service` | Warehouse team | Stock levels and reservations |
| `payment-service` | Payments team | Payments; talks to our payment provider (PSP) |

Place-order flow (orchestrated saga, all steps over Kafka through per-service transactional outboxes):

1. `POST /orders` (customer from the JWT subject; idempotent per customer on `Idempotency-Key`) stores the order as `PENDING` and, in the same transaction, queues `ReserveStock`.
2. Inventory answers `StockReserved` or `StockRejected`. Orders with no reply after 15 minutes are cancelled by the saga watchdog, which also sends `ReleaseStock`.
3. On `StockReserved` the saga sends `ChargePayment`; payment answers `PaymentCaptured` or `PaymentFailed`.
4. `PaymentFailed` triggers `ReleaseStock` and the order is `CANCELLED`; `PaymentCaptured` confirms it.

Failed messages are retried with backoff for about 3 minutes, then dead-lettered to `<topic>-dlt`. Every DLT record increments `kafka_dlt_records_total`, which pages on-call.

Production: 3 replicas per service on k8s, peak ~300 orders/minute, expected to triple next year.
