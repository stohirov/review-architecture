# Lens rules

Every lens agent reads this file before its checklists, and follows it exactly. The parent uses the same Ownership table and Severity scale to merge and re-grade in step 5.

## Method

1. Read each checklist. Every rule in it is in scope.
2. For each rule, search for its signals, then read the surrounding code and follow the call path across files. A `@Transactional` method that calls a helper that calls a Feign client is a hit, even though no single file shows both. In design mode, read the brief for the rule's signals and for what the brief leaves unaddressed.
3. Before reporting a hit, look for the mitigations the rule lists, anywhere in scope.
4. **Stay in your lane.** Find each problem's owner in the Ownership table. If the owner is another lens that is running, leave the problem to it. If the owner is not running, report it yourself. Report a problem that no rule names only if your lens owns it and you can state its failure scenario.

Done when every rule in your checklists is marked `hit`, `clear` or `n/a`, each with a one-line reason.

## Ownership

When a root cause fits more than one row, its owner is the lens whose fix closes the failure. For example, a retried non-idempotent POST is closed by idempotency, so data-consistency owns it; an SMTP call inside a transaction is closed by moving it out of the transaction, so data-consistency owns it. Ownership follows the fix, not the impact: a PSP error that is misclassified as a decline is closed by error classification, so resilience owns it, even though the damage is a wrong charge. If it is still tied, the earlier row wins.

| Root cause | Owner |
|---|---|
| remote call inside a transaction; dual writes (DB plus broker, DB plus callback or API); multi-step flows needing saga or compensation; idempotency of consumers and endpoints; lost updates, races and deadlocks on shared rows (including lock ordering); input that corrupts invariants; transaction-boundary misuse | data-consistency |
| timeouts; circuit breakers; client-side retries and retry stacking; synchronous call chains; bulkheads; optional dependencies on the critical path | resilience |
| ack and offset handling (the duplicate *effect* of a redelivery belongs to data-consistency); DLQ and retry topics; ordering and partition keys; event schema and serialization; request/reply over a broker | messaging |
| instance-local state, files and caches; schedulers on every replica; slow work in the request path; unbounded results; contention points; backpressure; consumer and thread capacity | scalability |
| query shape and N+1; indexes and constraints; Open Session in View; schema migrations; connection pool sizing; table growth and retention; bulk writes | persistence |
| shared databases; distributed-monolith coupling; chatty interfaces; internal models leaking across a boundary; premature splits | service-boundaries |
| health, startup and readiness probes; graceful shutdown; config and secrets; tracing and correlation; metrics and alerts (including lag and DLT alerts); edge and auth boundary; k8s resources; rolling-deploy compatibility | operability |

## Severity

Grade by the worst realistic outcome. Then lower it one level for each independent failure, beyond the first, that must coincide for the outcome to happen. If infrastructure the repo doesn't show might *prevent* the failure (a LimitRange, an ingress policy), cap the confidence at medium, keep the severity, and say what would have to exist. In design mode, grade against the brief and the profile's assumptions.

Severity is **impact × trigger**. The damaging impacts are: data lost or corrupted, money or stock wrong, or the core flow down. The trigger decides between critical and high:

- **critical**: a damaging impact on the normal path, or on a frequent routine event: every request, every deploy or scale-out, any timeout under ordinary load. Also: blocks horizontal scaling outright when the project needs more than one instance.
  - Examples: every message fails to deserialize; a retried POST double-charges whenever the callee is slow; carts in instance memory behind a load balancer.
- **high**: the same impacts behind a less frequent trigger: a crash in the narrow window between two steps, a broker redelivery after a rebalance, a dependency outage. Also: wrong behaviour at the project's stated scale.
  - Examples: a crash between commit and publish loses the event; a redelivered message is applied twice; a hung dependency drains the pool.
- **medium**: degrades performance or operability now, breaks at roughly 10× today's load, or degrades as data accumulates at today's load (a table without an index, a log without retention).
- **low**: cheap hygiene with a real but small payoff.
- **info**: an observation with no action needed.

Critical is meant to be rare. Before you grade critical, name the trigger and check that it fires on the normal path or on a frequent routine event. A failure that needs a request in flight at the wrong moment, or a rare event, is high.

## Confidence

- **high**: you traced it end to end in code (or the brief states it outright). Framework default behaviour counts as traced when the build pins the version and you can name the default. If you could not check that default in source, say so in `evidence` ("framework default, Spring Kafka 3.2, from memory").
- **medium**: you saw the signal; part of the context is inferred.
- **low**: suspicion. Critical or high findings with low confidence are dropped by the parent, so dig until you can raise the confidence or lower the severity.

## Output

Return exactly these two parts and nothing else, as your final message or through the platform's hand-back mechanism:

1. **Rule ledger**: one line per checklist rule, `<rule-id>: hit | clear | n/a, <reason>`.
2. **Candidates**: one fenced `json` block holding an array of objects with these fields (leave out `id`; the parent assigns it):
   - `lens`
   - `title`: one line; wrap placeholders and identifiers in backticks (`` `<topic>-dlt` ``)
   - `severity`
   - `confidence`
   - `locations`:
     - codebase and hybrid: `[{ "file": "<repo-relative path>", "line": <n> }]`
     - a finding about something *missing*: point at the file where the setting or code belongs (if none exists, the closest build or deploy file), with `"section": "missing <what>"` instead of a line
     - design mode: `[{ "file": "<brief filename or 'conversation'>", "section": "<the brief's exact heading text>" }]`; for a gap, the heading of the section where the answer belongs
   - `evidence`: quoted code, or text quoted verbatim from the brief in double quotes (never from the profile; write the brief's own double quotes as single quotes inside your quote, and mark cuts with `...`); for a gap, say what the brief leaves out
   - `failure_scenario`: the concrete sequence of events that goes wrong
   - `recommendation`
   - `pattern`: a kebab-case name from your checklist's vocabulary, or from another lens's if it fits better; coin a new one when none fits, rather than reuse a name that means something else
   - `applies_when`: the condition in this project that makes the fix proportionate
   - `effort`: `small`, `medium` or `large`

Return `[]` when nothing survives.
