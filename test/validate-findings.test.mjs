import { test } from "node:test";
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { validate } from "../skills/review-architecture/scripts/validate-findings.mjs";

const SCRIPT = new URL("../skills/review-architecture/scripts/validate-findings.mjs", import.meta.url).pathname;

function finding(overrides = {}) {
  return {
    id: "DC-001",
    lens: "data-consistency",
    title: "HTTP call inside @Transactional",
    severity: "high",
    confidence: "high",
    locations: [{ file: "order-service/src/main/java/OrderService.java", line: 42 }],
    evidence: "paymentClient.charge(order) inside @Transactional placeOrder()",
    failure_scenario: "Payment succeeds, then the order insert rolls back; the customer is charged for no order.",
    recommendation: "Commit the order first, then call payment from an outbox-driven step.",
    pattern: "transactional-outbox",
    applies_when: "Order and payment live in separate services with separate databases.",
    effort: "medium",
    ...overrides,
  };
}

function doc(overrides = {}) {
  return {
    schema_version: 1,
    mode: "codebase",
    target: "fixtures/02",
    profile: { architecture: "microservices", summary: "Three Spring Boot services.", lenses_run: ["data-consistency"] },
    findings: [finding()],
    ...overrides,
  };
}

test("accepts a well-formed document", () => {
  assert.deepEqual(validate(doc()), []);
});

test("accepts an empty findings list", () => {
  assert.deepEqual(validate(doc({ findings: [] })), []);
});

test("rejects missing required fields", () => {
  const { evidence, ...rest } = finding();
  const errors = validate(doc({ findings: [rest] }));
  assert.ok(errors.some((e) => e.includes('missing required "evidence"')), errors.join("\n"));
});

test("rejects unknown enum values and extra properties", () => {
  const errors = validate(doc({ findings: [finding({ severity: "urgent", extra: 1 })] }));
  assert.ok(errors.some((e) => e.includes('"urgent" is not one of')));
  assert.ok(errors.some((e) => e.includes('unexpected property "extra"')));
});

test("rejects blank strings and bad patterns", () => {
  const errors = validate(doc({ findings: [finding({ title: "   ", pattern: "Transactional Outbox", id: "dc-1" })] }));
  assert.ok(errors.some((e) => e.includes("$.findings[0].title: must not be blank")));
  assert.ok(errors.some((e) => e.includes("$.findings[0].pattern")));
  assert.ok(errors.some((e) => e.includes("$.findings[0].id")));
});

test("rejects duplicate ids", () => {
  const errors = validate(doc({ findings: [finding(), finding()] }));
  assert.ok(errors.some((e) => e.includes("duplicate id DC-001")));
});

test("rejects high severity with low confidence", () => {
  const errors = validate(doc({ findings: [finding({ confidence: "low" })] }));
  assert.ok(errors.some((e) => e.includes("need medium or high confidence")));
});

test("codebase locations need a line or section", () => {
  const errors = validate(doc({ findings: [finding({ locations: [{ file: "a.java" }] })] }));
  assert.ok(errors.some((e) => e.includes("need a line or section")));
});

test("design locations need the brief section", () => {
  const errors = validate(doc({ mode: "design", findings: [finding({ locations: [{ file: "brief.md", line: 3 }] })] }));
  assert.ok(errors.some((e) => e.includes("need the brief section")));
  const ok = validate(doc({ mode: "design", findings: [finding({ locations: [{ file: "brief.md", section: "Payments" }] })] }));
  assert.deepEqual(ok, []);
});

test("finding lens must be one of the lenses that ran", () => {
  const errors = validate(doc({ findings: [finding({ id: "SC-001", lens: "scalability" })] }));
  assert.ok(errors.some((e) => e.includes('"scalability" is not in profile.lenses_run')));
});

test("rejects wrong schema version and non-integer line", () => {
  const errors = validate(doc({ schema_version: 2, findings: [finding({ locations: [{ file: "a", line: 1.5 }] })] }));
  assert.ok(errors.some((e) => e.includes("$.schema_version")));
  assert.ok(errors.some((e) => e.includes("expected integer")));
});

test("CLI exits 0 for valid and 1 for invalid files", () => {
  const dir = mkdtempSync(join(tmpdir(), "ra-validate-"));
  const good = join(dir, "good.json");
  const bad = join(dir, "bad.json");
  writeFileSync(good, JSON.stringify(doc()));
  writeFileSync(bad, JSON.stringify(doc({ mode: "nope" })));
  assert.match(execFileSync("node", [SCRIPT, good], { encoding: "utf8" }), /valid \(1 finding/);
  assert.throws(() => execFileSync("node", [SCRIPT, bad], { stdio: "pipe" }), (err) => err.status === 1);
});
