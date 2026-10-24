#!/usr/bin/env node
// Validates architecture-findings.json against ../findings-schema.json.
// Usage: node validate-findings.mjs <path-to-findings.json> [--report <architecture-review.md>] [--brief <brief.md>]
// Interprets the subset of JSON Schema the schema uses, then applies
// cross-field checks that are clearer in code.

import { readFileSync } from "node:fs";
import { basename, dirname, join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const SCHEMA_PATH = join(dirname(fileURLToPath(import.meta.url)), "..", "findings-schema.json");
const SUPPORTED = new Set([
  "$schema", "$comment", "description", "type", "required", "additionalProperties",
  "properties", "items", "enum", "const", "pattern", "minLength", "minItems", "minimum",
]);

export function loadSchema() {
  return JSON.parse(readFileSync(SCHEMA_PATH, "utf8"));
}

function typeOf(value) {
  if (value === null) return "null";
  if (Array.isArray(value)) return "array";
  if (Number.isInteger(value)) return "integer";
  return typeof value;
}

function matchesType(value, type) {
  const actual = typeOf(value);
  return actual === type || (type === "number" && actual === "integer");
}

function checkNode(value, schema, path, errors) {
  for (const key of Object.keys(schema)) {
    if (!SUPPORTED.has(key)) throw new Error(`schema keyword "${key}" at ${path} is not supported`);
  }
  if (schema.type && !matchesType(value, schema.type)) {
    errors.push(`${path}: expected ${schema.type}, got ${typeOf(value)}`);
    return;
  }
  if ("const" in schema && value !== schema.const) {
    errors.push(`${path}: expected ${JSON.stringify(schema.const)}`);
  }
  if (schema.enum && !schema.enum.includes(value)) {
    errors.push(`${path}: ${JSON.stringify(value)} is not one of ${schema.enum.join(", ")}`);
  }
  if (typeof value === "string") {
    if (schema.minLength !== undefined && value.trim().length < schema.minLength) {
      errors.push(`${path}: must not be blank`);
    }
    if (schema.pattern && !new RegExp(schema.pattern, "u").test(value)) {
      errors.push(`${path}: ${JSON.stringify(value)} does not match ${schema.pattern}`);
    }
  }
  if (typeof value === "number" && schema.minimum !== undefined && value < schema.minimum) {
    errors.push(`${path}: must be >= ${schema.minimum}`);
  }
  if (Array.isArray(value)) {
    if (schema.minItems !== undefined && value.length < schema.minItems) {
      errors.push(`${path}: needs at least ${schema.minItems} item(s)`);
    }
    if (schema.items) value.forEach((item, i) => checkNode(item, schema.items, `${path}[${i}]`, errors));
  }
  if (typeOf(value) === "object") {
    const props = schema.properties ?? {};
    for (const key of schema.required ?? []) {
      if (!(key in value)) errors.push(`${path}: missing required "${key}"`);
    }
    for (const [key, child] of Object.entries(value)) {
      if (props[key]) checkNode(child, props[key], `${path}.${key}`, errors);
      else if (schema.additionalProperties === false) errors.push(`${path}: unexpected property "${key}"`);
    }
  }
}

function crossFieldChecks(doc, errors) {
  if (!Array.isArray(doc?.findings)) return;
  const seen = new Set();
  doc.findings.forEach((f, i) => {
    const at = `$.findings[${i}]`;
    if (typeof f?.id === "string") {
      if (seen.has(f.id)) errors.push(`${at}.id: duplicate id ${f.id}`);
      seen.add(f.id);
    }
    if ((f?.severity === "critical" || f?.severity === "high") && f?.confidence === "low") {
      errors.push(`${at}: ${f.severity} findings need medium or high confidence; verify or downgrade`);
    }
    if ((doc.mode === "codebase" || doc.mode === "hybrid") && Array.isArray(f?.locations)) {
      f.locations.forEach((loc, j) => {
        if (loc && loc.line === undefined && loc.section === undefined) {
          errors.push(`${at}.locations[${j}]: codebase findings need a line or section`);
        }
      });
    }
    if (doc.mode === "design" && Array.isArray(f?.locations)) {
      f.locations.forEach((loc, j) => {
        if (loc && loc.section === undefined) {
          errors.push(`${at}.locations[${j}]: design findings need the brief section`);
        }
      });
    }
  });
  const ids = new Set(doc.findings.map((f) => f?.id));
  (Array.isArray(doc.triage) ? doc.triage : []).forEach((t, i) => {
    const at = `$.triage[${i}]`;
    if (t?.action === "dropped") {
      if (t.finding !== undefined) errors.push(`${at}: a dropped candidate names no finding`);
      return;
    }
    if (t?.action !== "merged" && t?.action !== "regraded") return;
    if (!ids.has(t.finding)) errors.push(`${at}.finding: ${t.action} candidates must name an existing finding`);
    if (t.action === "regraded" && (t.from !== undefined || t.to !== undefined) && (t.from === undefined || t.to === undefined || t.from === t.to)) {
      errors.push(`${at}: a severity re-grade needs different from and to severities`);
    }
  });
  const lensesRun = doc.profile?.lenses_run;
  if (Array.isArray(lensesRun)) {
    doc.findings.forEach((f, i) => {
      if (f?.lens && !lensesRun.includes(f.lens)) {
        errors.push(`$.findings[${i}].lens: "${f.lens}" is not in profile.lenses_run`);
      }
      (Array.isArray(f?.also_reported_by) ? f.also_reported_by : []).forEach((lens) => {
        if (!lensesRun.includes(lens)) errors.push(`$.findings[${i}].also_reported_by: "${lens}" is not in profile.lenses_run`);
      });
    });
  }
}

function escapeRegExp(s) {
  return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

const REQUIRED_SECTIONS = ["summary", "architecture profile", "findings", "roadmap", "what's already sound", "not reviewed"];

function normalizeHeading(text) {
  return text.trim().toLowerCase().replace(/[‘’]/g, "'");
}

// Yields each line with its ATX heading, if any; lines inside fenced code blocks are never headings.
function* markdownLines(markdown) {
  let fence = null;
  for (const line of markdown.split(/\r?\n/u)) {
    const marker = /^\s{0,3}(`{3,}|~{3,})/u.exec(line)?.[1];
    if (marker && (fence === null || (marker[0] === fence[0] && marker.length >= fence.length))) {
      fence = fence === null ? marker : null;
    }
    const m = fence === null && !marker ? /^(#{1,6})\s+(.*?)\s*#*\s*$/u.exec(line) : null;
    yield { line, heading: m ? { level: m[1].length, text: m[2] } : null };
  }
}

// Splits the report into "## " sections and records which "### " severity group
// each "#### <id>" finding heading sits under (REPORT.md defines the layout).
function parseReport(markdown) {
  const sections = new Map();
  const findingHeadings = new Map();
  let h2 = null;
  let h3 = null;
  for (const { line, heading } of markdownLines(markdown)) {
    if (heading) {
      if (heading.level <= 2) {
        h2 = heading.level === 2 ? normalizeHeading(heading.text) : null;
        h3 = null;
        if (h2 !== null) sections.set(h2, sections.get(h2) ?? []);
      } else if (heading.level === 3) {
        h3 = normalizeHeading(heading.text);
      } else if (heading.level === 4) {
        const id = /^([A-Z]{2,5}-[0-9]{3})\b/u.exec(heading.text.trim())?.[1];
        if (id && !findingHeadings.has(id)) findingHeadings.set(id, { h2, h3 });
      }
    }
    if (h2 !== null) sections.get(h2).push(line);
  }
  return { sections, findingHeadings };
}

export function checkReport(doc, markdown) {
  if (!Array.isArray(doc?.findings)) return [];
  const { sections, findingHeadings } = parseReport(markdown);
  const errors = REQUIRED_SECTIONS.filter((s) => !sections.has(s)).map((s) => `report: missing section "## ${s}"`);
  const roadmap = (sections.get("roadmap") ?? []).join("\n");
  for (const f of doc.findings) {
    if (typeof f?.id !== "string") continue;
    const heading = findingHeadings.get(f.id);
    if (!heading) {
      errors.push(`report: no "#### ${f.id}" heading`);
      continue;
    }
    if (heading.h2 !== "findings") errors.push(`report: ${f.id} is not under "## Findings"`);
    else if (heading.h3 !== f.severity) errors.push(`report: ${f.id} is under "### ${heading.h3 ?? "(none)"}" but its severity is ${f.severity}`);
    if (f.severity !== "info" && sections.has("roadmap") && !new RegExp(`\\b${escapeRegExp(f.id)}\\b`, "u").test(roadmap)) {
      errors.push(`report: ${f.id} is missing from the Roadmap`);
    }
  }
  return errors;
}

const MIN_QUOTE_LENGTH = 15;
const LIST_MARKER = /^\s*(?:[-*+]|\d+[.)])\s+/u;

// Markdown emphasis and quote characters vary between a brief and a quotation of it.
function normalizeQuote(text) {
  return text.replace(/[`*_"'“”‘’]/g, "").replace(/\s+/g, " ").trim().toLowerCase();
}

// Passages inside double quotes in the evidence, split at elisions ("..." or "…");
// fragments shorter than MIN_QUOTE_LENGTH are too ambiguous to check.
function quotedSegments(evidence) {
  return [...evidence.matchAll(/["“]([^"“”]+)["”]/gu)]
    .flatMap((m) => m[1].split(/\.{3}|…/u))
    .map((q) => q.replace(LIST_MARKER, ""))
    .filter((q) => normalizeQuote(q).length >= MIN_QUOTE_LENGTH);
}

// Design mode: every location that cites the brief must name one of its real headings,
// and every quoted passage in the evidence must appear verbatim in the brief.
export function checkBrief(doc, briefMarkdown, briefName) {
  if (!Array.isArray(doc?.findings)) return [];
  const headings = new Set();
  for (const { heading } of markdownLines(briefMarkdown)) if (heading) headings.add(normalizeHeading(heading.text));
  // Quotes may run across list items, so line-leading bullets and numbers are ignored.
  const brief = normalizeQuote(briefMarkdown.replace(new RegExp(LIST_MARKER.source, "gmu"), ""));
  const errors = [];
  for (const f of doc.findings) {
    for (const quote of typeof f?.evidence === "string" ? quotedSegments(f.evidence) : []) {
      if (!brief.includes(normalizeQuote(quote))) errors.push(`brief: ${f.id} quotes "${quote.slice(0, 60)}", which is not in ${briefName}`);
    }
    for (const loc of Array.isArray(f?.locations) ? f.locations : []) {
      if (typeof loc?.file !== "string" || basename(loc.file) !== briefName || typeof loc.section !== "string") continue;
      if (!headings.has(normalizeHeading(loc.section))) errors.push(`brief: ${f.id} cites section "${loc.section}", which is not a heading in ${briefName}`);
    }
  }
  return errors;
}

export function validate(doc, schema = loadSchema()) {
  const errors = [];
  checkNode(doc, schema, "$", errors);
  crossFieldChecks(doc, errors);
  return errors;
}

function main(argv) {
  const args = argv.slice(2);
  const option = (name) => {
    const idx = args.indexOf(name);
    if (idx < 0) return undefined;
    return args.splice(idx, 2)[1] ?? "";
  };
  const reportPath = option("--report");
  const briefPath = option("--brief");
  const file = args[0];
  if (!file || args.length !== 1 || reportPath === "" || briefPath === "") {
    console.error("usage: validate-findings.mjs <path-to-findings.json> [--report <architecture-review.md>] [--brief <brief.md>]");
    return 2;
  }
  let doc;
  try {
    doc = JSON.parse(readFileSync(file, "utf8"));
  } catch (err) {
    console.error(`${file}: ${err.message}`);
    return 1;
  }
  const errors = validate(doc);
  if (reportPath) {
    try {
      errors.push(...checkReport(doc, readFileSync(reportPath, "utf8")));
    } catch (err) {
      errors.push(`${reportPath}: ${err.message}`);
    }
  }
  if (briefPath) {
    try {
      errors.push(...checkBrief(doc, readFileSync(briefPath, "utf8"), basename(briefPath)));
    } catch (err) {
      errors.push(`${briefPath}: ${err.message}`);
    }
  }
  if (errors.length) {
    for (const e of errors) console.error(e);
    console.error(`${file}: ${errors.length} error(s)`);
    return 1;
  }
  const checked = [reportPath && `report layout ok in ${reportPath}`, briefPath && `brief sections ok in ${briefPath}`].filter(Boolean);
  console.log(`${file}: valid (${doc.findings.length} finding(s))${checked.map((c) => `; ${c}`).join("")}`);
  return 0;
}

if (import.meta.url === pathToFileURL(process.argv[1] ?? "").href) {
  process.exitCode = main(process.argv);
}
