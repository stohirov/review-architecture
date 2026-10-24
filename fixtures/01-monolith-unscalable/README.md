# Shopfront

Online store for our home-goods brand. Spring Boot monolith: catalog, cart, checkout, order history, product images, monthly invoices.

## Running

```
./mvnw spring-boot:run
```

## Production

Runs on our k8s cluster (`k8s/`). We currently run 3 replicas behind the ingress and the HPA scales to 12 for campaigns. Marketing expects roughly 20x normal traffic for Black Friday (peak around 1,500 req/s), so we are reviewing the architecture before then.
