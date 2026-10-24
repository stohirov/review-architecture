# Eval run r5

The skill was at commit `ba76865`, tuned on r4. Control fixture 03 was at commit `12f875d`, with JWT auth and PSP decline handling. This was a confirmation run on the control and the design brief. The new `--brief` check ran end to end on 04.

| Fixture | Recall (must-detect) | Control | Severity mix (crit/high/med/low/info) | Result |
|---|---|---|---|---|
| 03-microservices-healthy | n/a | 1 high | 0 / 1 / 7 / 11 / 0 | FAIL |
| 04-design-brief | 6/6 | n/a | 2 / 14 / 9 / 2 / 0 | PASS |

**The control's high is a regression in the r4 fixture fix.** DC-001: payment-service treated *every* PSP 4xx as a final card decline. That included 401/403, where a bad API key would silently cancel every order, and 409/429, where a retry or rate limit could cancel an order that was charged. The fixture now treats only 400, 402 and 422 as final declines. Every other 4xx propagates, so it is retried, then dead-lettered and paged.

The pattern across r1–r5 is consistent: each fix made to the control fixture by hand was audited by the next review round, and two of the fixes had subtle defects of their own.
