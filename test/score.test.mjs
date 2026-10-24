import { test } from "node:test";
import assert from "node:assert/strict";
import { readdirSync, readFileSync } from "node:fs";
import { matches, score, formatReport } from "../evals/score.mjs";

function finding(overrides = {}) {
  return {
    id: "DC-001",
    lens: "data-consistency",
    title: "Payment call inside @Transactional",
    severity: "high",
    confidence: "high",
    locations: [{ file: "order-service/src/main/java/x/OrderService.java", line: 10 }],
    evidence: "paymentClient.charge(...) inside @Transactional placeOrder",
    failure_scenario: "Charge succeeds, insert rolls back.",
    recommendation: "Commit first, then call payment via the outbox.",
    pattern: "transactional-outbox",
    applies_when: "Separate services and databases.",
    effort: "medium",
    ...overrides,
  };
}

function doc(findings, overrides = {}) {
  return {
    schema_version: 1,
    mode: "codebase",
    target: "fixture",
    profile: { architecture: "microservices", summary: "test" },
    findings,
    ...overrides,
  };
}

const key = {
  fixture: "test",
  mode: "codebase",
  expected: [
    { key: "http-in-tx", lenses: ["data-consistency"], files: ["OrderService.java"], keywords: ["transaction", "payment"], must_detect: true },
    { key: "no-timeouts", lenses: ["resilience"], files: ["PaymentClient.java"], keywords: ["timeout"], must_detect: true },
    { key: "tracing", lenses: ["operability"], files: ["pom.xml"], keywords: ["trac"], must_detect: false },
  ],
};

test("matches on lens, file suffix and every keyword", () => {
  const expected = key.expected[0];
  assert.equal(matches(finding(), expected), true);
  assert.equal(matches(finding({ lens: "resilience" }), expected), false);
  assert.equal(matches(finding({ locations: [{ file: "OtherService.java", line: 1 }] }), expected), false);
  assert.equal(matches(finding({ title: "x", evidence: "x", failure_scenario: "x", recommendation: "x" }), expected), false);
});

test("file match respects path segment boundaries", () => {
  const expected = { lenses: ["data-consistency"], files: ["OrderService.java"], keywords: [] };
  assert.equal(matches(finding({ locations: [{ file: "src/MyOrderService.java", line: 1 }] }), expected), false);
  assert.equal(matches(finding({ locations: [{ file: "./OrderService.java", line: 1 }] }), expected), true);
});

test("computes recall and lists misses", () => {
  const r = score(doc([finding()]), key);
  assert.equal(r.recall, 0.5);
  assert.deepEqual(r.missed, ["no-timeouts"]);
  assert.equal(r.pass, false);
});

test("passes when every must-detect key is found and reports bonus and unmatched", () => {
  const r = score(
    doc([
      finding(),
      finding({ id: "RS-001", lens: "resilience", title: "No timeout on Feign", locations: [{ file: "a/PaymentClient.java", line: 3 }], pattern: "timeouts" }),
      finding({ id: "OP-001", lens: "operability", title: "No tracing", locations: [{ file: "pom.xml", line: 1 }], pattern: "distributed-tracing", severity: "medium" }),
      finding({ id: "SC-001", lens: "scalability", title: "Something else", locations: [{ file: "b.java", line: 1 }], pattern: "pagination", severity: "low" }),
    ]),
    key,
  );
  assert.equal(r.pass, true);
  assert.equal(r.recall, 1);
  assert.deepEqual(r.bonus.map((b) => b.key), ["tracing"]);
  assert.equal(r.unmatched.length, 1);
  assert.match(formatReport(r), /PASS/);
});

test("control keys fail on severity above the cap and on forbidden patterns", () => {
  const control = { fixture: "control", mode: "codebase", expected: [], control: { max_severity: "medium", forbidden_patterns: ["transactional-outbox"] } };
  const r = score(doc([finding()]), control);
  assert.deepEqual(r.control.overSeverity, ["DC-001"]);
  assert.deepEqual(r.control.forbiddenPattern, ["DC-001"]);
  assert.equal(r.pass, false);
  const clean = score(doc([finding({ severity: "low", pattern: "data-retention" })]), control);
  assert.equal(clean.pass, true);
});

test("invalid findings files fail with validation errors", () => {
  const r = score(doc([finding({ severity: "urgent" })]), key);
  assert.equal(r.pass, false);
  assert.ok(r.validationErrors.length > 0);
  assert.match(formatReport(r), /invalid findings file/);
});

test("mode must match the answer key, hybrid counts as codebase", () => {
  assert.equal(score(doc([], { mode: "design" }), { ...key, expected: [] }).modeMismatch, true);
  assert.equal(score(doc([], { mode: "hybrid" }), { ...key, expected: [] }).modeMismatch, false);
});

test("every answer key is well-formed", () => {
  const dir = new URL("../evals/answer-keys/", import.meta.url);
  const fixtures = new Set(readdirSync(new URL("../fixtures/", import.meta.url)));
  for (const name of readdirSync(dir)) {
    const k = JSON.parse(readFileSync(new URL(name, dir), "utf8"));
    assert.equal(`${k.fixture}.json`, name);
    assert.ok(fixtures.has(k.fixture), `${name}: no fixture directory`);
    assert.ok(["codebase", "design", "hybrid"].includes(k.mode));
    const keys = new Set();
    for (const e of k.expected) {
      assert.ok(!keys.has(e.key), `${name}: duplicate key ${e.key}`);
      keys.add(e.key);
      assert.ok(e.lenses.length && e.files.length, `${name}/${e.key}: needs lenses and files`);
      assert.equal(typeof e.must_detect, "boolean");
      (e.keywords ?? []).forEach((kw) => new RegExp(kw, "iu"));
    }
    assert.ok(k.expected.length > 0 || k.control, `${name}: needs expected findings or a control block`);
  }
});
