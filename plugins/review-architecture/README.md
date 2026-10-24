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

Source, test fixtures and eval results: https://github.com/stohirov/review-architecture

License: MIT
