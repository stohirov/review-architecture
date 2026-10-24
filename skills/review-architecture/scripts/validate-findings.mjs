#!/usr/bin/env node
// Validates architecture-findings.json against ../findings-schema.json.
// Usage: node validate-findings.mjs <path-to-findings.json> [--report <architecture-review.md>]
// Interprets the subset of JSON Schema the schema uses, then applies
// cross-field checks that are clearer in code.

import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
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
  const lensesRun = doc.profile?.lenses_run;
  if (Array.isArray(lensesRun)) {
    doc.findings.forEach((f, i) => {
      if (f?.lens && !lensesRun.includes(f.lens)) {
        errors.push(`$.findings[${i}].lens: "${f.lens}" is not in profile.lenses_run`);
      }
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

// Splits the report into "## " sections and records which "### " severity group
// each "#### <id>" finding heading sits under (REPORT.md defines the layout).
function parseReport(markdown) {
  const sections = new Map();
  const findingHeadings = new Map();
  let h2 = null;
  let h3 = null;
  let fence = null;
  for (const line of markdown.split(/\r?\n/u)) {
    const marker = /^\s{0,3}(`{3,}|~{3,})/u.exec(line)?.[1];
    if (marker && (fence === null || (marker[0] === fence[0] && marker.length >= fence.length))) {
      fence = fence === null ? marker : null;
    }
    const m = fence === null && !marker ? /^(#{1,6})\s+(.*)$/u.exec(line) : null;
    if (m) {
      const level = m[1].length;
      if (level <= 2) {
        h2 = level === 2 ? normalizeHeading(m[2]) : null;
        h3 = null;
        if (h2 !== null) sections.set(h2, sections.get(h2) ?? []);
      } else if (level === 3) {
        h3 = normalizeHeading(m[2]);
      } else if (level === 4) {
        const id = /^([A-Z]{2,5}-[0-9]{3})\b/u.exec(m[2].trim())?.[1];
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

export function validate(doc, schema = loadSchema()) {
  const errors = [];
  checkNode(doc, schema, "$", errors);
  crossFieldChecks(doc, errors);
  return errors;
}

function main(argv) {
  const args = argv.slice(2);
  const reportIdx = args.indexOf("--report");
  const reportPath = reportIdx >= 0 ? args.splice(reportIdx, 2)[1] : undefined;
  const file = args[0];
  if (!file || args.length !== 1 || (reportIdx >= 0 && !reportPath)) {
    console.error("usage: validate-findings.mjs <path-to-findings.json> [--report <architecture-review.md>]");
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
  if (errors.length) {
    for (const e of errors) console.error(e);
    console.error(`${file}: ${errors.length} error(s)`);
    return 1;
  }
  console.log(`${file}: valid (${doc.findings.length} finding(s))${reportPath ? `; report layout ok in ${reportPath}` : ""}`);
  return 0;
}

if (import.meta.url === pathToFileURL(process.argv[1] ?? "").href) {
  process.exitCode = main(process.argv);
}
