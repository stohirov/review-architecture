#!/usr/bin/env node
// Scores an architecture-findings.json against a fixture answer key.
// Usage: node evals/score.mjs <findings.json> <answer-key.json> [--min-recall 0.9] [--report <review.md>] [--brief <brief.md>]
// Exits 0 when the findings are valid, recall meets the bar and no control finding exceeds the
// key's max severity. Control findings that name an already-implemented pattern are listed as
// warnings: they are false positives unless they point at a concrete gap in that implementation.

import { readFileSync } from "node:fs";
import { pathToFileURL } from "node:url";
import { basename } from "node:path";
import { validate, checkReport, checkBrief } from "../plugins/review-architecture/skills/review-architecture/scripts/validate-findings.mjs";

const SEVERITY_RANK = { info: 0, low: 1, medium: 2, high: 3, critical: 4 };

function normalizePath(p) {
  return p.replaceAll("\\", "/").replace(/^\.\//, "");
}

function fileMatches(locationFile, expectedFile) {
  const loc = normalizePath(locationFile);
  const want = normalizePath(expectedFile);
  return loc === want || loc.endsWith(`/${want}`);
}

function searchableText(finding) {
  return [finding.title, finding.evidence, finding.failure_scenario, finding.recommendation, finding.pattern].join("\n");
}

export function matches(finding, expected) {
  if (!expected.lenses.includes(finding.lens)) return false;
  const inFile = finding.locations.some((loc) => expected.files.some((file) => fileMatches(loc.file, file)));
  if (!inFile) return false;
  const text = searchableText(finding);
  return (expected.keywords ?? []).every((kw) => new RegExp(kw, "iu").test(text));
}

export function score(doc, key, { minRecall = 0.9, report, brief } = {}) {
  const validationErrors = validate(doc);
  if (!validationErrors.length && report !== undefined) validationErrors.push(...checkReport(doc, report));
  if (!validationErrors.length && brief !== undefined) validationErrors.push(...checkBrief(doc, brief.text, brief.name));
  const findings = validationErrors.length ? [] : doc.findings;
  const matchedIds = new Set();
  const detected = [];
  const missed = [];
  const bonus = [];

  for (const expected of key.expected) {
    const hits = findings.filter((f) => matches(f, expected)).map((f) => f.id);
    hits.forEach((id) => matchedIds.add(id));
    if (hits.length) (expected.must_detect ? detected : bonus).push({ key: expected.key, by: hits });
    else if (expected.must_detect) missed.push(expected.key);
  }

  const mustTotal = key.expected.filter((e) => e.must_detect).length;
  const recall = mustTotal === 0 ? 1 : detected.length / mustTotal;

  const control = { overSeverity: [], forbiddenPattern: [] };
  if (key.control) {
    const cap = SEVERITY_RANK[key.control.max_severity];
    const forbidden = new Set(key.control.forbidden_patterns ?? []);
    for (const f of findings) {
      if (SEVERITY_RANK[f.severity] > cap) control.overSeverity.push(f.id);
      if (forbidden.has(f.pattern)) control.forbiddenPattern.push(f.id);
    }
  }

  const modeMismatch = !validationErrors.length && key.mode && doc.mode !== key.mode && !(key.mode === "codebase" && doc.mode === "hybrid");
  const pass =
    validationErrors.length === 0 &&
    !modeMismatch &&
    recall >= minRecall &&
    control.overSeverity.length === 0;

  return {
    fixture: key.fixture,
    pass,
    validationErrors,
    modeMismatch,
    recall,
    mustTotal,
    detected,
    missed,
    bonus,
    control,
    unmatched: findings.filter((f) => !matchedIds.has(f.id)).map((f) => `${f.id} [${f.severity}] ${f.title}`),
    findingCount: findings.length,
  };
}

export function formatReport(r) {
  const lines = [`${r.fixture}: ${r.pass ? "PASS" : "FAIL"}`];
  if (r.validationErrors.length) {
    lines.push(`  invalid findings file (${r.validationErrors.length} error(s)):`);
    r.validationErrors.slice(0, 10).forEach((e) => lines.push(`    ${e}`));
    return lines.join("\n");
  }
  if (r.modeMismatch) lines.push("  mode does not match the answer key");
  lines.push(`  findings: ${r.findingCount}`);
  lines.push(`  recall: ${r.detected.length}/${r.mustTotal} (${(r.recall * 100).toFixed(0)}%)`);
  r.detected.forEach((d) => lines.push(`    + ${d.key} <- ${d.by.join(", ")}`));
  r.missed.forEach((k) => lines.push(`    - MISSED ${k}`));
  if (r.bonus.length) lines.push(`  bonus: ${r.bonus.map((b) => b.key).join(", ")}`);
  if (r.control.overSeverity.length) lines.push(`  control: above max severity: ${r.control.overSeverity.join(", ")}`);
  if (r.control.forbiddenPattern.length) lines.push(`  control warning (check by hand): names an implemented pattern: ${r.control.forbiddenPattern.join(", ")}`);
  if (r.unmatched.length) {
    lines.push("  not in answer key (review by hand):");
    r.unmatched.forEach((u) => lines.push(`    ? ${u}`));
  }
  return lines.join("\n");
}

function main(argv) {
  const args = argv.slice(2);
  const option = (name) => {
    const idx = args.indexOf(name);
    return idx >= 0 ? args.splice(idx, 2)[1] : undefined;
  };
  const minRecallArg = option("--min-recall");
  const minRecall = minRecallArg === undefined ? 0.9 : Number(minRecallArg);
  const reportPath = option("--report");
  const briefPath = option("--brief");
  if (args.length !== 2 || Number.isNaN(minRecall)) {
    console.error("usage: score.mjs <findings.json> <answer-key.json> [--min-recall 0.9] [--report <review.md>] [--brief <brief.md>]");
    return 2;
  }
  const [findingsPath, keyPath] = args;
  const doc = JSON.parse(readFileSync(findingsPath, "utf8"));
  const key = JSON.parse(readFileSync(keyPath, "utf8"));
  const result = score(doc, key, {
    minRecall,
    report: reportPath && readFileSync(reportPath, "utf8"),
    brief: briefPath && { text: readFileSync(briefPath, "utf8"), name: basename(briefPath) },
  });
  console.log(formatReport(result));
  return result.pass ? 0 : 1;
}

if (import.meta.url === pathToFileURL(process.argv[1] ?? "").href) {
  process.exitCode = main(process.argv);
}
