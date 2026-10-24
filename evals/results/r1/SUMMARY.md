# Eval run r1

Skill at commit `786c7f6`. Each fixture was reviewed by one agent following `SKILL.md`, which fanned out lens subagents in parallel. Each fixture was copied to an isolated workspace first, so no reviewer could see the answer keys.

| Fixture | Recall (must-detect) | Control | Findings | Result |
|---|---|---|---|---|
| 01-monolith-unscalable | 9/9 | n/a | 26 | PASS |
| 02-microservices-distributed-tx | 10/11 (missed `sync-call-chain`) | n/a | 24 | PASS |
| 03-microservices-healthy | n/a | 4 high findings | 24 | FAIL |
| 04-design-brief | 6/6 | n/a | 22 | PASS |

## The control failure was a fixture bug, not a skill bug

All four high findings on the "healthy" fixture are real defects, confirmed by reading the code:

- **DC-001**: `SagaWatchdog` reloads a PENDING order in a new transaction, but `Order.cancel()` accepts `STOCK_RESERVED`. A reply that lands between the list query and the reload cancels an order whose `ChargePayment` is already in the outbox.
- **DC-002**: no validation of order lines. A negative quantity passes `available >= :qty` and increases stock.
- **MQ-001**: about 15 s of container backoff is shorter than the PSP circuit breaker's 30 s open window, so a short PSP outage dead-letters charges, and nothing alerts on or replays the DLTs.
- **SC-001**: the payment consumer runs one thread per pod, partitions are undeclared and `max.poll.records` is 500, against a blocking PSP call.

Several medium findings were also real and subtle. For example, **DC-004**: a checked `JsonProcessingException` does not roll back `@Transactional`, so the inbox row commits and a replayed DLT message is silently skipped.

The fixture was fixed after this run; see the next commits.

## Skill feedback reported by all four runs

1. The JSON `profile` had no place for stated intent, key flows, runtime or assumptions, although RECON/INTAKE require them.
2. Lenses overlap with no ownership map, so the same root cause came back from 3–5 lenses and merging dominated step 5.
3. REPORT.md left no heading level for the severity groups.
4. Nothing checked that every JSON finding appears in the Markdown.
5. There was no pattern name for common fixes: atomic conditional update, input validation, lock ordering, resource requests/PDB, DTO boundary.
6. Codebase vs hybrid was ambiguous when the README states target scale.
7. There was no fallback when some lens agents cannot launch (concurrency limit).
8. Critical vs high overlapped for "money wrong under failure"; there was no likelihood rule.
