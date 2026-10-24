# Eval run r2

The skill was at commit `030def6`, after tuning on r1. Control fixture 03 was at commit `b8e432f`, after its r1 bugs were fixed. The setup was the same as r1: isolated workspaces, one reviewing agent per fixture. Under the new small-codebase rule, every run fanned out to 3 grouped lens agents instead of 6–7.

| Fixture | Recall (must-detect) | Control | Findings | Result |
|---|---|---|---|---|
| 01-monolith-unscalable | 9/9 | n/a | 24 | PASS |
| 02-microservices-distributed-tx | 11/11 (r1 missed `sync-call-chain`) | n/a | 28 | PASS |
| 03-microservices-healthy | n/a | 3 high findings | 21 | FAIL |
| 04-design-brief | 6/6 | n/a | 30 | PASS |

Every run passed `validate-findings.mjs --report`. All runs also found bugs that were not planted in the fixtures. The most notable is in 02: the producer's JSON type header names a class the consumer does not have, so Spring Kafka's defaults make every event fail to deserialize. Both r1 and r2 found it.

## The control failed again on real bugs

The three highs on 03 are real. Two of them were introduced by the r1 fixes:

- **MQ-001**: `DeadLetterPublishingRecoverer` defaults to `<topic>.DLT`, while the r1 fix created, monitored and paged on `<topic>-dlt`.
- **DC-001**: the charged amount comes from client-supplied unit prices.
- **SC-001**: there was no cap on order lines, so one oversized outbox row could block the relay for every order.

All three are fixed in the fixture (commit `6cc71d2`). This is the expected dynamic for a control fixture: each review round finds subtler real defects, and the "healthy" baseline gets harder to fault.

## r2 feedback addressed afterwards

- **Ownership ties**: when a root cause fits several rows, the lens whose fix closes the failure owns it.
- **Roadmap buckets**: redefined so Next is never accidentally empty and a prerequisite of any severity can go in Now.
- **Unseen infrastructure**: when it might *prevent* a failure, lower the confidence and keep the severity.
- **Design-mode evidence**: it must quote the brief verbatim, and gaps point at the section where the answer belongs.
- **Non-interactive runs**: they take the most likely mode and record assumptions.
- **Strengths**: the parent writes `strengths` from rules marked clear because a mitigation exists.
