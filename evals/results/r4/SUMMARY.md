# Eval run r4

The skill was at commit `17fd545`, with severity recalibrated as impact × trigger, the stricter `--report` layout check and `also_reported_by`. Control fixture 03 was at commit `6cc71d2`. All four fixtures ran.

| Fixture | Recall (must-detect) | Control | Severity mix (crit/high/med/low/info) | Result |
|---|---|---|---|---|
| 01-monolith-unscalable | 9/9 | n/a | 6 / 10 / 10 / 2 / 1 | PASS |
| 02-microservices-distributed-tx | 11/11 | n/a | 2 / 16 / 9 / 2 / 0 | PASS |
| 03-microservices-healthy | n/a | 1 high | 0 / 1 / 10 / 10 / 0 | FAIL |
| 04-design-brief | 6/6 | n/a | 2 / 11 / 13 / 1 / 0 | PASS |

Every run passed the new `--report` layout check.

**Severity calibration worked.** Fixture 02 had 10 criticals in r3 and 7 in r2; it has 2 in r4. Both are normal-path failures: every event fails to deserialize, and the retried POST double-charges whenever payment-service is slow. Narrow-window failures such as a crash between commit and publish moved to high.

**The control's one high is real.** OP-001: nothing authenticates callers, and the charged `customerId` comes from the request body. The fixture now takes the customer from a verified JWT. A medium that was also real (DC-001: a PSP 4xx decline is retried and dead-lettered instead of producing `PaymentFailed`) is fixed too.
