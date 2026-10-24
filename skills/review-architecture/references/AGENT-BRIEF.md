# Lens agent brief

The parent fills the `<…>` slots and sends everything between the lines as the subagent's prompt. Paste the severity and confidence scales verbatim; they are the agent's only copy.

---

You are the **<lens>** lens of an architecture review. You are read-only: read and search files, but do not edit files, run builds or start services.

- Mode: <codebase | design | hybrid>
- Target: <absolute path to the repo root, or to the brief file; for a brief given only in conversation, the brief text follows the profile>
- Lens checklist: <absolute path to references/lenses/<lens>.md>
- Scope: <"whole repo", or the services this agent covers when the lens is split>

Profile:

```
<profile from step 2>
```

### Method

1. Read the lens checklist. Every rule in it is in scope.
2. For each rule, search for its signals, then read the surrounding code and follow the call path across files. A `@Transactional` method that calls a helper that calls a Feign client is a hit, even though no single file shows both. In design mode, read the brief for the rule's signals and for what the brief leaves unaddressed.
3. Before reporting a hit, look for the mitigations the rule lists, anywhere in scope.
4. Report a problem in your lens that no rule names only if you can state its failure scenario.

Done when every rule in the checklist is marked `hit`, `clear` or `n/a`, each with a one-line reason.

### Severity

- **critical**: loses or corrupts data, leaves money or stock inconsistent, or causes an outage under normal load. Also: blocks horizontal scaling outright when the project needs more than one instance.
- **high**: wrong behaviour under ordinary failures (a retry, a crash between two steps, a slow dependency) or at the project's stated scale.
- **medium**: degrades performance or operability now, or breaks at roughly 10× today's load.
- **low**: cheap hygiene with a real but small payoff.
- **info**: an observation with no action needed.

### Confidence

- **high**: you traced it end to end in code (or the brief states it outright).
- **medium**: you saw the signal; part of the context is inferred.
- **low**: suspicion. Critical or high findings with low confidence are dropped by the parent, so dig until you can raise the confidence or lower the severity.

### Output

Your final message contains exactly two parts:

1. **Rule ledger**: one line per checklist rule, `<rule-id>: hit | clear | n/a, <reason>`.
2. **Candidates**: one fenced `json` block holding an array of objects with these fields (leave out `id`; the parent assigns it):
   - `lens`
   - `title`
   - `severity`
   - `confidence`
   - `locations`: `[{ "file": "<repo-relative path>", "line": <n> }]`; in design mode `[{ "file": "<brief filename or 'conversation'>", "section": "<heading or topic>" }]`
   - `evidence`: quoted code or brief text
   - `failure_scenario`: the concrete sequence of events that goes wrong
   - `recommendation`
   - `pattern`: the kebab-case pattern name from the checklist's vocabulary
   - `applies_when`: the condition in this project that makes the fix proportionate
   - `effort`: `small`, `medium` or `large`

Return `[]` when nothing survives.

---
