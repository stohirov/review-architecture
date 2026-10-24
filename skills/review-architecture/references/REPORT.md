# Report structure

`architecture-review.md` is for the engineers who own the system. It must make sense to someone who never saw the review run. Every finding in `architecture-findings.json` appears here under the same id.

## Sections, in order

The `##` headings are exactly: `## Summary`, `## Architecture profile`, `## Findings`, `## Roadmap`, `## What's already sound` and `## Not reviewed`. The validator's `--report` check requires them, and checks that each finding sits under its own severity heading and that every non-info finding appears in the Roadmap.

1. **Title and scope**: `# Architecture review: <project name>`, then the mode, the date, the lenses that ran (and why any were skipped), any lens the parent had to run itself, and how many candidates were merged or dropped.
2. **Summary**: 3–5 sentences: what the system is, its biggest risk, and the one change that buys the most. In hybrid mode, add one sentence on whether the system can meet its stated intent. Then a severity count table.
3. **Architecture profile**: the profile from step 2, including the assumptions, plus one Mermaid diagram of the deployables, datastores, brokers and external systems (third-party APIs, identity provider, anything else that writes to a datastore) as they *are*, or as the brief plans them. Mark every edge sync or async, and label risky edges with their finding ids (`A -- "sync · RS-001" --> B`). Highlight the nodes that findings touch with `classDef risk stroke:#d33,stroke-width:3px`. Quote any label that contains parentheses or braces.
4. **Findings**: under `## Findings`, one `### Critical` / `### High` / `### Medium` / `### Low` / `### Info` heading per severity that has findings, in that order. Each finding gets:
   - `#### <id> · <title>`, and a line with severity, confidence, lens, effort and `also_reported_by` (if any)
   - **Where**: locations as `path:line` (design mode: the brief section)
   - **Evidence**: the quoted code or brief text, in a fenced block
   - **What goes wrong**: the failure scenario
   - **Fix**: the recommendation and pattern, with a short code sketch when the fix is local
   - **Why it's worth it here**: `applies_when`
   - For a fix that changes how data flows (outbox, saga, idempotent consumer), add a small Mermaid sequence diagram of the fixed flow. Findings with the same fixed flow share one diagram. Use at most four sequence diagrams per report (the architecture diagram in section 3 does not count).
5. **Roadmap**: findings ordered into
   - **Now**: critical and high with small or medium effort, plus any finding (of any severity) their fixes depend on
   - **Next**: the remaining critical and high, then the medium findings
   - **Later**: low findings, and info findings that carry an action

   Inside each bucket, put a finding before the findings whose fix depends on it ("unblocks" means exactly this: a hard dependency), then order by severity. Write "nothing" when an item unblocks nothing. A cheap stopgap that protects users until a larger fix lands may go in Now, marked "stopgap for <id>".

   One line per item: id, the action, and what it unblocks.
6. **What's already sound**: the `strengths` from the JSON. Name the patterns done right, so nobody "fixes" them.
7. **Not reviewed**: anything out of scope or unreadable (generated code, missing services, secrets and security in depth), and the candidates in `dropped` with their reasons. Point to a security review for security.
