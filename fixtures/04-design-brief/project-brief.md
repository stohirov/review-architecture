# RideNow: architecture brief (draft v0.3)

## What we're building

RideNow is a ride-hailing app for three cities at launch (about 4M people combined). Riders request a ride, we match them with a nearby driver, track the trip live, and charge the rider's card when the trip ends.

## Targets

- Launch: 15,000 active drivers at peak, 120,000 rides per day.
- Year two: 50,000 active drivers, 400,000 rides per day, plus two more cities.
- Peak hour (Friday night) is about 4× the daily average.
- Availability target for requesting a ride: 99.95%.
- Team: 4 squads of about 6 engineers, each owning two services and shipping independently.

## Services

We are going microservices from day one so the squads can move independently:

1. **api-gateway**: routing and auth.
2. **rider-service**: rider profiles and saved payment methods.
3. **driver-service**: driver profiles, documents, availability status.
4. **location-service**: receives driver GPS pings and answers "where is driver X".
5. **matching-service**: finds the best nearby available driver for a ride request.
6. **booking-service**: owns the ride lifecycle (requested → matched → in progress → completed).
7. **payment-service**: card pre-authorization and capture through Stripe.
8. **notification-service**: push notifications and SMS.

## Data

To keep things simple and avoid data duplication, all eight services connect to one shared PostgreSQL cluster (one primary, one read replica) with a single `ridenow` schema. Services can join across each other's tables when they need data, for example booking-service joins `drivers` and `riders` directly to build the trip receipt.

## Booking flow

1. The rider app calls `POST /bookings`.
2. booking-service creates the ride row with status `REQUESTED`.
3. booking-service calls payment-service synchronously, which calls Stripe to pre-authorize the estimated fare. If Stripe is slow, the request waits; we will set a generous timeout so we don't lose bookings.
4. booking-service calls matching-service synchronously to get a driver. matching-service keeps the list of available drivers in an in-memory map that driver-service updates through a REST callback whenever a driver goes online or offline. We'll run one matching-service instance at launch and add more later.
5. booking-service calls notification-service synchronously to push "driver on the way" to the rider and the driver.
6. booking-service updates the ride to `MATCHED` and returns the result to the rider app.

All of steps 2–6 run inside one database transaction in booking-service, so if anything fails the booking is rolled back cleanly.

## Trip completion

When the driver ends the trip, booking-service sets the ride to `COMPLETED`, then publishes a `TripCompleted` event to Kafka right after the database commit. payment-service consumes it and captures the final fare. Receipts and driver earnings are computed by consumers of the same event.

## Live location

Drivers' apps send a GPS ping every 2 seconds while online. location-service writes each ping as an `UPDATE drivers SET lat = ?, lng = ? WHERE id = ?` on the shared Postgres. The rider app polls `GET /bookings/{id}/driver-location` every second during a trip, which reads the same row. matching-service also queries this table to find nearby drivers (`ORDER BY` distance computed in SQL).

## Not decided yet

- How we deploy (probably Kubernetes).
- Observability tooling.
