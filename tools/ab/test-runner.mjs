#!/usr/bin/env node
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { readdirSync, readFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
const root=resolve(dirname(new URL(import.meta.url).pathname),"../..");
const runner=join(root,"tools/ab/ab-runner.mjs");
const noKey=spawnSync(process.execPath,[runner,"--scenario","controlled-tool-loop"],{encoding:"utf8",env:{PATH:process.env.PATH}});
assert.equal(noKey.status,2); assert.match(`${noKey.stdout || ""}${noKey.stderr || ""}${noKey.error?.message || ""}`,/missing OPENCODE_GO_KEY/);
const scenarios=join(root,"tools/ab/scenarios");
for(const name of readdirSync(scenarios)){
  const dir=join(scenarios,name), spec=JSON.parse(readFileSync(join(dir,"scenario.json"),"utf8"));
  assert.equal(spec.id,name); assert.ok(spec.task_file); assert.ok(spec.initial_snapshot_sha256); assert.ok(spec.acceptance.command); assert.ok(spec.review_rubric.length>=3);
  const accept=spawnSync("sh",["-c",spec.acceptance.command],{cwd:join(dir,"seed"),encoding:"utf8"});
  assert.notEqual(accept.status,0,`${name} seed must not already pass acceptance`);
}
console.log("A/B runner contract tests passed");
