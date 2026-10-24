# Eval run r6

The skill was at commit `2826f60`: lens rules moved to `LENS-RULES.md`, so lens prompts went from about 110 lines to about 20, and `dropped` became `triage`. Control fixture 03 was at commit `9e48865`. Scoring used the full checks (`--report`, plus `--brief` for 04).

| Fixture | Recall (must-detect) | Control | Severity mix (crit/high/med/low/info) | Triage entries | Result |
|---|---|---|---|---|---|
| 02-microservices-distributed-tx | 11/11 | n/a | 3 / 13 / 11 / 0 / 0 | 5 | PASS |
| 03-microservices-healthy | n/a | 1 high | 0 / 1 / 9 / 9 / 0 | 2 | FAIL |
| 04-design-brief | 6/6 | n/a | 2 / 13 / 10 / 0 / 0 | 5 | PASS |

Recall held with the shorter prompts.

**The control's high is in code added by the r4 fixture fix.** RS-001: JWT validation via `issuer-uri` fetches the identity provider's discovery document and JWK set with Spring Security's default client, which has no timeouts. A stalled identity provider hangs request threads and health probes. The reviewer flagged this as "framework default, from memory", as the r4 confidence rule now asks. The fixture now builds its own `JwtDecoder` with connect and read timeouts.
