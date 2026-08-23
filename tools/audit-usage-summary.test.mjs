#!/usr/bin/env node
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";

const directory = mkdtempSync(join(tmpdir(), "orihsus-audit-summary-"));
const file = join(directory, "audit.jsonl");
writeFileSync(file, [
  { timestamp: "2026-08-15T10:00:00Z", gateway_key: "coding-agent", model: "deepseek-chat", input_tokens: 100, cached_tokens: 70, uncached_tokens: 30, output_tokens: 20 },
  { timestamp: "2026-08-15T11:00:00Z", gateway_key: "coding-agent", model: "deepseek-chat", input_tokens: 50, cached_tokens: 10, uncached_tokens: 40, output_tokens: 15 },
  { timestamp: "2026-08-16T00:00:00Z", gateway_key: "hiyori", model: "deepseek-chat", input_tokens: 999, cached_tokens: 0, uncached_tokens: 999, output_tokens: 1 },
].map(JSON.stringify).join("\n") + "\n");
const script = join(dirname(new URL(import.meta.url).pathname), "audit-usage-summary.mjs");
const output = JSON.parse(execFileSync(process.execPath, [script, "--file", file, "--from", "2026-08-15T00:00:00Z", "--to", "2026-08-16T00:00:00Z"], { encoding: "utf8" }));
assert.deepEqual(output.rows, [{ gateway_key: "coding-agent", model: "deepseek-chat", requests: 2, input_tokens: 150, cached_tokens: 80, uncached_tokens: 70, output_tokens: 35 }]);
console.log("audit usage summary tests passed");
