#!/usr/bin/env node
import { readFileSync } from "node:fs";

function fail(message) {
  process.stderr.write(`audit-usage-summary: ${message}\n`);
  process.exit(2);
}

const args = { file: "/var/log/orihsus/audit.jsonl", from: null, to: null };
for (let i = 2; i < process.argv.length; i++) {
  const arg = process.argv[i];
  if (arg === "--file") args.file = process.argv[++i];
  else if (arg === "--from") args.from = new Date(process.argv[++i]);
  else if (arg === "--to") args.to = new Date(process.argv[++i]);
  else if (arg === "--help") {
    console.log("Usage: node tools/audit-usage-summary.mjs [--file PATH] [--from ISO-8601] [--to ISO-8601]");
    process.exit(0);
  } else fail(`unknown argument: ${arg}`);
}
if (!args.file || (args.from && Number.isNaN(args.from.valueOf())) || (args.to && Number.isNaN(args.to.valueOf()))) {
  fail("--file and valid ISO-8601 --from/--to values are required");
}

let text;
try { text = readFileSync(args.file, "utf8"); }
catch (error) { fail(`cannot read audit file: ${error.message}`); }

const groups = new Map();
for (const [index, line] of text.split(/\r?\n/).entries()) {
  if (!line) continue;
  let record;
  try { record = JSON.parse(line); }
  catch { fail(`invalid JSON at line ${index + 1}`); }
  const timestamp = new Date(record.timestamp);
  if (Number.isNaN(timestamp.valueOf())) continue;
  if (args.from && timestamp < args.from) continue;
  if (args.to && timestamp >= args.to) continue;
  if (typeof record.gateway_key !== "string" || typeof record.model !== "string") continue;
  const key = `${record.gateway_key}\0${record.model}`;
  const group = groups.get(key) ?? {
    gateway_key: record.gateway_key, model: record.model, requests: 0,
    input_tokens: 0, cached_tokens: 0, uncached_tokens: 0, output_tokens: 0,
  };
  group.requests++;
  for (const field of ["input_tokens", "cached_tokens", "uncached_tokens", "output_tokens"]) {
    if (typeof record[field] === "number" && Number.isSafeInteger(record[field]) && record[field] >= 0) {
      group[field] += record[field];
    }
  }
  groups.set(key, group);
}

const rows = [...groups.values()].sort((a, b) =>
  a.gateway_key.localeCompare(b.gateway_key) || a.model.localeCompare(b.model));
process.stdout.write(`${JSON.stringify({ file: args.file, from: args.from?.toISOString() ?? null, to: args.to?.toISOString() ?? null, rows }, null, 2)}\n`);
