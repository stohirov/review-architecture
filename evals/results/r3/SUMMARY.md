# Eval run r3

The skill was at commit `007392a` (tuned on r2) and control fixture 03 at commit `6cc71d2` (r2 bugs fixed). This was a confirmation run on the two microservices fixtures only.

| Fixture | Recall (must-detect) | Control | Findings | Result |
|---|---|---|---|---|
| 02-microservices-distributed-tx | 11/11 | n/a | 27 | PASS |
| 03-microservices-healthy | n/a | 0 critical, 0 high | 20 (12 medium, 8 low) | PASS |

The control's 7 pattern warnings were checked by hand. Each points at a concrete gap in an existing implementation, not at a missing pattern. Examples:

- the circuit breaker does not pause the consumer, so a long PSP outage dead-letters the head of each partition;
- the DLT counter is created at its first increment, so rate alerts miss the first dead letter;
- the order idempotency key is not scoped to the customer.

None of them recommends adding something that is already there.

## Fixed after this run

- The validator skipped the line/section check for `hybrid` mode.
- The brief told agents to keep severity and lower confidence for failures that unseen infrastructure might prevent, while step 5 drops high findings with low confidence. Confidence is now capped at medium in that case.
- The severity rule now lowers one level for *each* extra independent failure that must coincide.

## Open items reported by agents and not yet addressed

- Severity inflation on money and stock: r3 graded 10 of 27 findings critical on fixture 02, while r2 graded 7 of 28.
- `--report` checks headings only. It does not check the severity table, the required sections or roadmap coverage.
- Merged candidates are not recorded anywhere in the JSON.
- Remembered framework defaults (Spring Kafka, Feign, Boot) cannot be verified when the run is restricted to the project and skill.
