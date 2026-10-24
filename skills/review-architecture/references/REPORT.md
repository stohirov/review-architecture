# Report structure

`architecture-review.md` is for the engineers who own the system. It must make sense to someone who never saw the review run. Every finding in `architecture-findings.json` appears here under the same id.

## Sections, in order

1. **Title and scope**: `# Architecture review: <target>`, then the mode, the date and the lenses that ran (and why any were skipped).
2. **Summary**: 3–5 sentences: what the system is, its biggest risk, and the one change that buys the most. Then a severity count table.
3. **Architecture profile**: the profile from step 2, plus one Mermaid diagram of the deployables, datastores and brokers as they *are* (or as the brief plans them). Mark every edge sync or async. Highlight the edges and nodes that findings touch, for example with `classDef risk stroke:#d33,stroke-width:3px`.
4. **Findings**: grouped by severity, critical first. Each finding gets:
   - `### <id> · <title>` and a line with severity, confidence, lens and effort
   - **Where**: locations as `path:line` (design mode: the brief section)
   - **Evidence**: the quoted code or brief text, in a fenced block
   - **What goes wrong**: the failure scenario
   - **Fix**: the recommendation and pattern, with a short code sketch when the fix is local
   - **Why it's worth it here**: `applies_when`
   - For a fix that changes how data flows (outbox, saga, idempotent consumer), add a small Mermaid sequence diagram of the fixed flow.
5. **Roadmap**: findings ordered into
   - **Now**: critical and high, small or medium effort
   - **Next**: the remaining critical and high, plus medium findings that unblock them
   - **Later**: the rest

   One line per item: id, the action, and what it unblocks.
6. **What's already sound**: the `strengths` from the JSON. Name the patterns done right, so nobody "fixes" them.
7. **Not reviewed**: anything out of scope or unreadable (generated code, missing services, secrets and security in depth). Point to a security review for security.
