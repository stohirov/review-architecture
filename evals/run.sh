#!/usr/bin/env bash
# Runs the review-architecture skill headlessly against one fixture and scores the result.
# Usage: evals/run.sh <fixture-name> [--prepare-only]
#
# The fixture is copied to a fresh temp directory first, so the reviewing agent
# never sees the answer keys or the rest of this repo.
set -euo pipefail

repo="$(cd "$(dirname "$0")/.." && pwd)"
fixture="${1:?usage: evals/run.sh <fixture-name> [--prepare-only]}"
src="$repo/fixtures/$fixture"
key="$repo/evals/answer-keys/$fixture.json"
[[ -d "$src" ]] || { echo "no fixture: $src" >&2; exit 2; }
[[ -f "$key" ]] || { echo "no answer key: $key" >&2; exit 2; }

TMPDIR="${TMPDIR:-/tmp}"
work="$(mktemp -d "${TMPDIR%/}/ra-eval-$fixture.XXXXXX")"
cp -R "$src/." "$work/"
echo "workspace: $work"
[[ "${2:-}" == "--prepare-only" ]] && exit 0

mode="$(node -e 'console.log(JSON.parse(require("fs").readFileSync(process.argv[1])).mode)' "$key")"
if [[ "$mode" == "design" ]]; then
  prompt="Review the architecture described in project-brief.md using the review-architecture skill. Nobody is available to answer intake questions: record your assumptions in the profile and continue. Write the report files to the current directory."
else
  prompt="Review the architecture of this codebase using the review-architecture skill. Nobody is available to answer questions: decide and continue. Write the report files to the project root."
fi

(cd "$work" && claude -p "$prompt" --plugin-dir "$repo" --permission-mode acceptEdits)

out="$repo/evals/results/$fixture"
mkdir -p "$out"
cp "$work/architecture-findings.json" "$work/architecture-review.md" "$out/"
node "$repo/evals/score.mjs" "$out/architecture-findings.json" "$key" | tee "$out/score.txt"
