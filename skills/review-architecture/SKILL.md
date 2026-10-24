---
name: review-architecture
description: Architecture review of a codebase or a described project, covering scalability, data consistency, resilience, service boundaries, messaging, persistence and operability, with proportional fixes. Use when the user asks to review, audit or critique an architecture or system design, asks whether a project will scale, describes a planned system for feedback, or asks about microservice pitfalls such as sagas, outbox, distributed transactions or HTTP calls inside transactions.
---

# Review Architecture

Find the system-design mistakes that will hurt this project at its real scale, prove each one with evidence, and recommend the smallest pattern that fixes it. A finding is a concrete **failure scenario** rooted in the code or the brief. A best-practice gap with no failure scenario is not a finding.

**Proportionality** governs every recommendation. Each finding names the condition in *this* project (`applies_when`) that makes the fix worth its cost. A saga in a single-database monolith, a broker for fifty requests a day, or microservices for a three-person team are wrong answers even when they are textbook patterns.

Everything below is relative to this skill's directory (the folder holding this file). Resolve it to an absolute path once; subagents need absolute paths.

- `references/INTAKE.md`: design-mode questions
- `references/RECON.md`: codebase profiling and the profile format
- `references/AGENT-BRIEF.md`: the brief each lens agent receives, including the severity scale
- `references/REPORT.md`: report structure
- `references/lenses/<lens>.md`: one checklist per lens
- `findings-schema.json` and `scripts/validate-findings.mjs`: the output contract and its validator

## 1. Choose the mode

- **design**: the user describes a project or points at a brief, RFC or diagram, and there is no code to review.
- **codebase**: the skill runs inside a repo with source code and the user gave no description.
- **hybrid**: there is code and a stated intent (target scale, planned split, an RFC). Review the code, and also check it against the intent.

If the request fits none of these cleanly, ask one question.

Done when the mode is decided.

## 2. Profile

- codebase or hybrid: follow `references/RECON.md`.
- design: follow `references/INTAKE.md`, which also produces a profile.

Done when every profile field is filled or marked `unknown` and the architecture is classified.

## 3. Select lenses

| Lens | Run when |
|---|---|
| `scalability` | always |
| `persistence` | a datastore exists or is planned |
| `operability` | codebase or hybrid mode; design mode when deployment is described |
| `data-consistency` | anything writes to a datastore |
| `resilience` | any network call to another service, broker or third party |
| `service-boundaries` | more than one deployable, or a monolith with several business modules |
| `messaging` | a broker or event stream is present or planned |

Record the chosen lenses as `profile.lenses_run`. Done when every lens is marked run or skip, with a one-line reason for each skip.

## 4. Fan out lens agents

Start one subagent per selected lens, **all in a single message** so they run in parallel. Build each prompt from `references/AGENT-BRIEF.md`.

For a large codebase (more than about 6 deployables or 400 source files), split `data-consistency` and `resilience` per group of services, and keep the total at about 10 agents or fewer.

If the platform has no subagent tool, run each lens yourself in turn with the same brief.

Done when every lens agent has returned its rule ledger and candidate list (the list may be empty).

## 5. Verify and merge

Lens agents each see one lens and one slice of code. You see everything. For every candidate:

1. **Evidence**: open the cited location. The quoted evidence must be there. Correct the line number, or drop the candidate.
2. **Mitigation**: search for a fix that lives elsewhere, such as a lock configured in another module, a global client timeout, an outbox relay, or a dedup table. If one exists, drop or downgrade the candidate.
3. **Proportionality**: check `applies_when` against the profile. Drop recommendations the project's scale or shape does not justify.
4. **Duplicates**: when two lenses report one root cause, keep the finding under the lens that owns the fix and fold the other's angle into it.
5. **Severity**: re-grade against the scale in `references/AGENT-BRIEF.md`. A critical or high finding needs medium or high confidence after your own reading.

Assign ids by lens prefix and sequence: `SC` scalability, `DC` data-consistency, `RS` resilience, `SB` service-boundaries, `MQ` messaging, `PS` persistence, `OP` operability (for example `DC-001`).

Done when you have personally re-read the evidence for every surviving critical or high finding, and every candidate is kept, merged or dropped.

## 6. Report

Write two files:
- `architecture-findings.json`, following `findings-schema.json`
- `architecture-review.md`, following `references/REPORT.md`

In codebase and hybrid mode they go in the project root; in design mode, the current directory. Use another location if the user names one.

Run `node <skill-dir>/scripts/validate-findings.mjs <path>/architecture-findings.json` and fix the file until it exits 0.

Done when the validator passes and every finding in the JSON appears in the Markdown report.

Close in chat with: the architecture in one line, finding counts by severity, the top three findings, and both file paths. Offer to go deeper on any finding or to implement a fix.
