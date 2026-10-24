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
- **hybrid**: there is code and a stated intent (target scale, planned split, an RFC), whether the user stated it or the repo's own README, docs or ADRs do. Review the code, and also check it against the intent.

If the request fits none of these cleanly, ask one question. If nobody can answer (a headless run), take the most likely mode and record it as an assumption.

Done when the mode is decided.

## 2. Profile

- codebase or hybrid: follow `references/RECON.md`.
- design: follow `references/INTAKE.md`, which also produces a profile.

Done when every profile field is filled or marked `unknown`, the architecture is classified, and every assumption you made is listed.

## 3. Select lenses

| Lens | Run when |
|---|---|
| `scalability` | always |
| `persistence` | a datastore exists or is planned |
| `operability` | codebase or hybrid mode; design mode when deployment is described or a likely platform is named |
| `data-consistency` | anything writes to a datastore |
| `resilience` | any network call to another service, broker or third party |
| `service-boundaries` | more than one deployable, or a monolith with several business modules |
| `messaging` | a broker or event stream is present or planned |

Record the chosen lenses as `profile.lenses_run`. Done when every lens is marked run or skip, with a one-line reason for each skip.

## 4. Fan out lens agents

Start the lens agents **all in a single message** so they run in parallel. Build each prompt from `references/AGENT-BRIEF.md`. Size the fan-out to the codebase:

- **Small** (under about 150 source files, or design mode): group the lenses into at most three agents, each running every checklist in its group. The groups are `data-consistency` + `messaging`, `resilience` + `service-boundaries`, and `scalability` + `persistence` + `operability`, minus any skipped lens.
- **Medium**: one agent per lens.
- **Large** (more than about 6 deployables or 400 source files): one agent per lens, with `data-consistency` and `resilience` also split per group of services. Keep the total at about 10 agents or fewer.

Keep working notes (the profile, raw candidate lists) in a temporary directory unique to this run, never in the project. Wait for every agent before step 5, and don't redo their work while they run.

If some agents are refused, for example by a concurrency limit, launch them as earlier ones finish. Run a lens yourself only if it still cannot launch, and say so in the report's scope line. If the platform has no subagent tool, run each lens yourself in turn with the same brief.

Done when every lens agent has returned its rule ledger and candidate list (the list may be empty).

## 5. Verify and merge

Lens agents each see one lens and one slice of code. You see everything. For every candidate:

1. **Evidence**: open the cited location. The quoted evidence must be there: in code, at that line (correct the line number if it is off); in design mode, verbatim in the brief. Otherwise drop the candidate.
2. **Mitigation**: search for a fix that lives elsewhere, such as a lock configured in another module, a global client timeout, an outbox relay, or a dedup table. If one exists, drop or downgrade the candidate.
3. **Proportionality**: check `applies_when` against the profile. Drop recommendations the project's scale or shape does not justify.
4. **Duplicates**: when two lenses report one root cause, keep the finding under its owner in the ownership table in `references/AGENT-BRIEF.md`, and fold the other angle into it.
5. **Severity**: re-grade against the scale in `references/AGENT-BRIEF.md`. A critical or high finding needs medium or high confidence after your own reading.

Assign ids by lens prefix and sequence: `SC` scalability, `DC` data-consistency, `RS` resilience, `SB` service-boundaries, `MQ` messaging, `PS` persistence, `OP` operability (for example `DC-001`).

Then write `strengths`: the patterns that are already done right, taken from rules the ledgers mark `clear` because a mitigation exists.

Done when you have personally re-read the evidence for every surviving critical or high finding, and every candidate is kept, merged or dropped.

## 6. Report

Write two files:
- `architecture-findings.json`, following `findings-schema.json`
- `architecture-review.md`, following `references/REPORT.md`

In codebase and hybrid mode they go in the project root; in design mode, the current directory. Use another location if the user names one.

Run `node <skill-dir>/scripts/validate-findings.mjs <path>/architecture-findings.json --report <path>/architecture-review.md`, and fix both files until it exits 0. The `--report` check confirms that every finding has its heading in the Markdown.

Done when the validator passes with `--report`.

Close in chat with: the architecture in one line, finding counts by severity, the top three findings, and both file paths. Offer to go deeper on any finding or to implement a fix.
