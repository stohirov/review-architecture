import { test } from "node:test";
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { validate, checkReport, checkBrief } from "../skills/review-architecture/scripts/validate-findings.mjs";

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
    profile: {
      architecture: "microservices",
      summary: "Three Spring Boot services.",
      stated_intent: "300 orders/min, 3x next year",
      assumptions: ["one team per service"],
      key_flows: ["order -> payment -> PSP"],
      lenses_run: ["data-consistency"],
    },
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

test("hybrid locations follow the codebase rule", () => {
  const errors = validate(doc({ mode: "hybrid", findings: [finding({ locations: [{ file: "a.java" }] })] }));
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

test("profile requires stated intent and rejects unknown fields", () => {
  const { stated_intent, ...profile } = doc().profile;
  assert.ok(validate(doc({ profile })).some((e) => e.includes('missing required "stated_intent"')));
  assert.ok(validate(doc({ profile: { ...doc().profile, mood: "ok" } })).some((e) => e.includes('unexpected property "mood"')));
});

function report({ findings = "### High\n#### DC-001 · HTTP call\n", roadmap = "- DC-001: move the call\n", drop = [] } = {}) {
  const sections = {
    "Summary": "Short.\n",
    "Architecture profile": "Profile.\n",
    "Findings": findings,
    "Roadmap": roadmap,
    "What's already sound": "Outbox.\n",
    "Not reviewed": "Security.\n",
  };
  return "# Architecture review: x\n" + Object.entries(sections)
    .filter(([name]) => !drop.includes(name))
    .map(([name, body]) => `## ${name}\n${body}`)
    .join("");
}

test("checkReport accepts the documented layout", () => {
  assert.deepEqual(checkReport(doc(), report()), []);
  assert.deepEqual(checkReport(doc(), report().replace("What's", "What’s")), []);
});

test("checkReport ignores headings inside code fences", () => {
  const body = "### High\n#### DC-001 · a\n```yaml\n# application.yml\n## not a heading\n```\n~~~\n# x\n~~~\n";
  assert.deepEqual(checkReport(doc(), report({ findings: body })), []);
  const hidden = report({ findings: "### High\n```\n#### DC-001 · a\n```\n" });
  assert.deepEqual(checkReport(doc(), hidden), ['report: no "#### DC-001" heading']);
});

test("checkReport requires every section", () => {
  assert.deepEqual(checkReport(doc(), report({ drop: ["Roadmap", "Not reviewed"] })), [
    'report: missing section "## roadmap"',
    'report: missing section "## not reviewed"',
  ]);
});

test("checkReport requires a #### heading per finding", () => {
  const d = doc({ findings: [finding(), finding({ id: "DC-002" })] });
  const errors = checkReport(d, report({ roadmap: "DC-001, DC-002\n", findings: "### High\n#### DC-001 · a\nsee DC-002 and DC-0021\n" }));
  assert.deepEqual(errors, ['report: no "#### DC-002" heading']);
});

test("checkReport requires findings under their own severity", () => {
  const errors = checkReport(doc(), report({ findings: "### Medium\n#### DC-001 · a\n" }));
  assert.deepEqual(errors, ['report: DC-001 is under "### medium" but its severity is high']);
  const outside = checkReport(doc(), report({ findings: "x\n" }) + "#### DC-001 · stray\n");
  assert.deepEqual(outside, ['report: DC-001 is not under "## Findings"']);
});

test("checkReport requires non-info findings in the roadmap", () => {
  assert.deepEqual(checkReport(doc(), report({ roadmap: "nothing\n" })), ["report: DC-001 is missing from the Roadmap"]);
  const info = doc({ findings: [finding({ severity: "info", confidence: "low" })] });
  assert.deepEqual(checkReport(info, report({ findings: "### Info\n#### DC-001 · a\n", roadmap: "nothing\n" })), []);
});

test("also_reported_by must name lenses", () => {
  assert.deepEqual(validate(doc({ findings: [finding({ also_reported_by: ["data-consistency"] })] })), []);
  assert.ok(validate(doc({ findings: [finding({ also_reported_by: ["security"] })] })).some((e) => e.includes('"security" is not one of')));
});

test("CLI --report fails when the layout is wrong", () => {
  const dir = mkdtempSync(join(tmpdir(), "ra-report-"));
  const json = join(dir, "f.json");
  const md = join(dir, "r.md");
  writeFileSync(json, JSON.stringify(doc()));
  writeFileSync(md, "# Review\n#### DC-001 · HTTP call\n");
  assert.throws(() => execFileSync("node", [SCRIPT, json, "--report", md], { stdio: "pipe" }), (err) => err.status === 1);
  writeFileSync(md, report());
  assert.match(execFileSync("node", [SCRIPT, json, "--report", md], { encoding: "utf8" }), /report layout ok/);
});

test("checkBrief requires design sections to be real brief headings", () => {
  const brief = "# RideNow\n## Booking flow\nText.\n```\n## Not a heading\n```\n## Live location ##\n";
  const d = doc({
    mode: "design",
    findings: [
      finding({ locations: [{ file: "project-brief.md", section: "booking flow" }] }),
      finding({ id: "DC-002", locations: [{ file: "docs/project-brief.md", section: "Live location" }, { file: "conversation", section: "anything" }] }),
      finding({ id: "DC-003", locations: [{ file: "project-brief.md", section: "Booking flow (step 4)" }] }),
      finding({ id: "DC-004", locations: [{ file: "project-brief.md", section: "Not a heading" }] }),
    ],
  });
  assert.deepEqual(checkBrief(d, brief, "project-brief.md"), [
    'brief: DC-003 cites section "Booking flow (step 4)", which is not a heading in project-brief.md',
    'brief: DC-004 cites section "Not a heading", which is not a heading in project-brief.md',
  ]);
});

test("also_reported_by lenses must have run", () => {
  const errors = validate(doc({ findings: [finding({ also_reported_by: ["resilience"] })] }));
  assert.ok(errors.some((e) => e.includes('"resilience" is not in profile.lenses_run')));
});

test("triage records drops, merges and re-grades", () => {
  const triage = [
    { action: "dropped", candidate: "persistence: CONCURRENTLY index", reason: "migration already ran" },
    { action: "merged", candidate: "resilience: SMTP on critical path", reason: "same root cause", finding: "DC-001" },
    { action: "regraded", candidate: "data-consistency: HTTP in tx", reason: "narrow window", finding: "DC-001", from: "critical", to: "high" },
  ];
  assert.deepEqual(validate(doc({ triage })), []);
  const errors = validate(doc({
    triage: [
      { action: "dropped", candidate: "a: b", reason: "c", finding: "DC-001" },
      { action: "merged", candidate: "a: b", reason: "c", finding: "DC-009" },
      { action: "regraded", candidate: "a: b", reason: "c", finding: "DC-001", from: "high", to: "high" },
      { action: "ignored", candidate: "a: b", reason: "c" },
    ],
  }));
  assert.ok(errors.some((e) => e.includes("a dropped candidate names no finding")));
  assert.ok(errors.some((e) => e.includes("must name an existing finding")));
  assert.ok(errors.some((e) => e.includes("need different from and to severities")));
  assert.ok(errors.some((e) => e.includes('"ignored" is not one of')));
});

test("checkBrief requires quoted evidence to be verbatim in the brief", () => {
  const brief = "## Data\nAll eight services connect to **one shared** PostgreSQL cluster.\nUses `UPDATE drivers SET lat = ?`.\n";
  const d = doc({
    mode: "design",
    findings: [
      finding({ locations: [{ file: "b.md", section: "Data" }], evidence: 'Brief: "all eight services connect to one shared   PostgreSQL" and “UPDATE drivers SET lat = ?” (no fallback given)' }),
      finding({ id: "DC-002", locations: [{ file: "b.md", section: "Data" }], evidence: '"every service has its own database" and "short"' }),
    ],
  });
  assert.deepEqual(checkBrief(d, brief, "b.md"), ['brief: DC-002 quotes "every service has its own database", which is not in b.md']);
});

test("checkBrief allows elisions and swapped inner quotes", () => {
  const brief = '## Services\n4. **location-service**: receives driver GPS pings and answers "where is driver X".\n5. matching-service keeps drivers in an in-memory map.\n';
  const d = doc({
    mode: "design",
    findings: [finding({ locations: [{ file: "b.md", section: "Services" }], evidence: `"4. location-service: receives driver GPS pings and answers 'where is driver X'. ... matching-service keeps drivers in an in-memory map … invented tail that is long"` })],
  });
  assert.deepEqual(checkBrief(d, brief, "b.md"), ['brief: DC-001 quotes " invented tail that is long", which is not in b.md']);
});
