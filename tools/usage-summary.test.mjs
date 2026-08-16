#!/usr/bin/env node
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";

const directory = mkdtempSync(join(tmpdir(), "orihsus-usage-summary-"));
writeFileSync(
  join(directory, "2026-08-15.jsonl"),
  [
    { timestamp: "2026-08-15T00:00:00Z", key_fingerprint: "abc123abc123", rolling: { percent: 20, resetsAt: "r1" }, weekly: { percent: 40 }, monthly: { percent: null } },
    { timestamp: "2026-08-15T00:05:00Z", key_fingerprint: "abc123abc123", rolling: { percent: 60, resetsAt: "r2" }, weekly: { percent: 50 }, monthly: { percent: 10 } },
  ].map(JSON.stringify).join("\n") + "\n",
);

const script = join(dirname(new URL(import.meta.url).pathname), "usage-summary.mjs");
const output = JSON.parse(execFileSync(process.execPath, [script, "--dir", directory, "--days", "2", "--end", "2026-08-16"], { encoding: "utf8" }));
assert.equal(output.rows.length, 1);
assert.deepEqual(output.rows[0], {
  date: "2026-08-15",
  key_fingerprint: "abc123abc123",
  samples: 2,
  rolling: { latest_percent: 60, max_percent: 60, average_percent: 40, resetsAt: "r2" },
  weekly: { latest_percent: 50, max_percent: 50, average_percent: 45, resetsAt: null },
  monthly: { latest_percent: 10, max_percent: 10, average_percent: 10, resetsAt: null },
});
console.log("usage summary tests passed");
