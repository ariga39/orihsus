#!/usr/bin/env node
import { readFileSync } from "node:fs";
import { join } from "node:path";

function fail(message) {
  process.stderr.write(`usage-summary: ${message}\n`);
  process.exit(2);
}

function parseArgs(argv) {
  const args = { dir: "/var/log/orihsus/usage", days: 7, end: new Date() };
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === "--dir") args.dir = argv[++i];
    else if (argv[i] === "--days") args.days = Number(argv[++i]);
    else if (argv[i] === "--end") args.end = new Date(`${argv[++i]}T00:00:00Z`);
    else if (argv[i] === "--help") {
      console.log("Usage: node tools/usage-summary.mjs [--dir DIR] [--days N] [--end YYYY-MM-DD]");
      process.exit(0);
    } else fail(`unknown argument: ${argv[i]}`);
  }
  if (!args.dir || !Number.isInteger(args.days) || args.days < 1 || Number.isNaN(args.end.valueOf())) {
    fail("--dir, a positive integer --days, and a valid --end are required");
  }
  return args;
}

const dayName = (date) => date.toISOString().slice(0, 10);
const args = parseArgs(process.argv.slice(2));
const groups = new Map();

for (let offset = args.days - 1; offset >= 0; offset--) {
  const date = new Date(args.end);
  date.setUTCDate(date.getUTCDate() - offset);
  const day = dayName(date);
  let text;
  try {
    text = readFileSync(join(args.dir, `${day}.jsonl`), "utf8");
  } catch (error) {
    if (error.code === "ENOENT") continue;
    fail(`cannot read ${day}.jsonl: ${error.message}`);
  }
  for (const [index, line] of text.split(/\r?\n/).entries()) {
    if (!line) continue;
    let record;
    try { record = JSON.parse(line); }
    catch { fail(`invalid JSON in ${day}.jsonl line ${index + 1}`); }
    if (typeof record.key_fingerprint !== "string") continue;
    const key = `${day}\0${record.key_fingerprint}`;
    const group = groups.get(key) ?? { date: day, key_fingerprint: record.key_fingerprint, samples: 0, windows: {} };
    group.samples++;
    for (const name of ["rolling", "weekly", "monthly"]) {
      const window = record[name] ?? {};
      const stats = group.windows[name] ?? { values: [], resetsAt: null, latest_percent: null };
      if (typeof window.percent === "number" && Number.isFinite(window.percent)) {
        stats.values.push(window.percent);
        stats.latest_percent = window.percent;
      }
      if (typeof window.resetsAt === "string") stats.resetsAt = window.resetsAt;
      group.windows[name] = stats;
    }
    groups.set(key, group);
  }
}

const rows = [...groups.values()].sort((a, b) => a.date.localeCompare(b.date) || a.key_fingerprint.localeCompare(b.key_fingerprint)).map(group => {
  const windows = {};
  for (const name of ["rolling", "weekly", "monthly"]) {
    const stats = group.windows[name] ?? { values: [], resetsAt: null, latest_percent: null };
    windows[name] = {
      latest_percent: stats.latest_percent,
      max_percent: stats.values.length ? Math.max(...stats.values) : null,
      average_percent: stats.values.length ? stats.values.reduce((sum, value) => sum + value, 0) / stats.values.length : null,
      resetsAt: stats.resetsAt,
    };
  }
  return { date: group.date, key_fingerprint: group.key_fingerprint, samples: group.samples, ...windows };
});

process.stdout.write(`${JSON.stringify({ directory: args.dir, days: args.days, rows }, null, 2)}\n`);
