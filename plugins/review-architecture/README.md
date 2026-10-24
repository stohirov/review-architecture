# review-architecture

A Claude Code skill that reviews a system's architecture and recommends fixes. Point it at a codebase, or describe a project you are planning. It profiles the system, sends parallel review agents through it, and finds the system-design mistakes that will hurt at your scale:

- instance-local state that blocks scaling out
- HTTP calls inside database transactions
- dual writes without an outbox
- multi-service flows without a saga
- non-idempotent consumers
- missing timeouts
- shared databases

It proves each finding with evidence, and recommends only fixes that are proportionate to the project. The output is `architecture-review.md` and a validated `architecture-findings.json`.

## Install

```
/plugin install review-architecture --marketplace stohirov/review-architecture
```

On Claude Code older than 2.1.275, add the marketplace first:

```
/plugin marketplace add stohirov/review-architecture
/plugin install review-architecture@review-architecture
```

## Use

Use it by asking "review the architecture of this repo", or with `/review-architecture:review-architecture`.

## What it runs and what it touches

- **Reads:** files in the project under review, or the design brief you give it, plus the skill's own reference files. Review agents are read-only.
- **Starts subagents:** parallel Claude review agents through your client's subagent tool, three for a small codebase and up to about ten for a large one. Without a subagent tool, it runs the reviews one after another.
- **Runs one local command:** `node <skill-dir>/scripts/validate-findings.mjs` checks the output files (and, in design mode, the brief). The script has no dependencies and makes no network requests.
- **Writes:** `architecture-findings.json` and `architecture-review.md` in the project root (in design mode, the current directory), or wherever you ask. Working notes go in a temporary directory for that run.
- **Does not:** make network requests, send data anywhere, install packages, run builds, tests or the project's code, or change Claude settings or permissions.
- **Personal data:** it doesn't collect any. If the reviewed code contains personal data, such as seed files, a finding may quote that code as evidence in the local report.

Source, test fixtures and eval results: https://github.com/stohirov/review-architecture

License: MIT
