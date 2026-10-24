# Order platform (v2)

Three Spring Boot services, each owning its own database:

| Service | Team | Owns |
|---|---|---|
| `order-service` | Checkout team | Orders and the order saga (orchestrator) |
| `inventory-service` | Warehouse team | Stock levels and reservations |
| `payment-service` | Payments team | Payments; talks to our payment provider (PSP) |

Place-order flow (orchestrated saga, all steps over Kafka through per-service transactional outboxes):

1. `POST /orders` stores the order as `PENDING` and emits `OrderCreated`.
2. Saga sends `ReserveStock`; inventory answers `StockReserved` or `StockRejected`.
3. On `StockReserved` the saga sends `ChargePayment`; payment answers `PaymentCaptured` or `PaymentFailed`.
4. `PaymentFailed` triggers `ReleaseStock` and the order is `CANCELLED`; `PaymentCaptured` confirms it.

Production: 3 replicas per service on k8s, peak ~300 orders/minute, expected to triple next year.
