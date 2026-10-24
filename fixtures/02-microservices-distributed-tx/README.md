# Order platform

Three Spring Boot services behind our gateway:

| Service | Team | Responsibility |
|---|---|---|
| `order-service` | Checkout team | Accepts orders, takes payment, publishes order events |
| `payment-service` | Payments team | Charges customers through our payment provider (PSP) |
| `inventory-service` | Warehouse team | Tracks stock, decrements it when an order is paid |

Each team deploys on its own schedule. Production runs 2 replicas of each service on k8s. Peak load is around 300 orders per minute, and we expect it to triple next year.

Local run: `docker compose up`.
