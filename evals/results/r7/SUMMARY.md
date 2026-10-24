# Eval run r7

The skill was at commit `e9021a4`, tuned on r6. Control fixture 03 was at commit `0baa857`, with JWKS fetch timeouts. This was a control-only confirmation run.

| Fixture | Control | Severity mix (crit/high/med/low/info) | Triage entries | Result |
|---|---|---|---|---|
| 03-microservices-healthy | 0 critical, 0 high | 0 / 0 / 11 / 11 / 0 | 3 | PASS |

The 7 control warnings (findings that name an already-implemented pattern) were checked by hand. Each points at a concrete gap in the existing implementation, not at a missing pattern:

- the PSP idempotency key can expire before a late DLT redrive;
- the circuit breaker does not pause the listener;
- DLT topics use the broker's default retention;
- the outbox drops trace context;
- order-service has no `preStop` delay.

This is the first pass on the current control fixture. Defects found in it by earlier rounds and since fixed:

| Round | Defects found (all real, all fixed in the fixture) |
|---|---|
| r1 | watchdog could cancel a reserved order; no input validation; retry budget shorter than the circuit-breaker window; undersized payment consumer |
| r2 | DLT topic name mismatch; client-supplied prices; unbounded order lines |
| r4 | no authentication; the charged customer came from the request body |
| r5 | every PSP 4xx treated as a decline (regression in the r4 fix) |
| r6 | JWKS fetch without timeouts (introduced by the r4 fix) |
