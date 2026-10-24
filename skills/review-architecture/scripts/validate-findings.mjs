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
    if (doc.mode === "codebase" && Array.isArray(f?.locations)) {
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

// Every finding needs its own Markdown heading, e.g. "#### DC-001 · Title".
export function checkReport(doc, markdown) {
  if (!Array.isArray(doc?.findings)) return [];
  return doc.findings
    .filter((f) => typeof f?.id === "string")
    .filter((f) => !new RegExp(`^#{2,6}\\s+${escapeRegExp(f.id)}\\b`, "mu").test(markdown))
    .map((f) => `report: no heading for ${f.id}`);
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
  console.log(`${file}: valid (${doc.findings.length} finding(s))${reportPath ? `; every finding has a heading in ${reportPath}` : ""}`);
  return 0;
}

if (import.meta.url === pathToFileURL(process.argv[1] ?? "").href) {
  process.exitCode = main(process.argv);
}
