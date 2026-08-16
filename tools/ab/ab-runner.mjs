#!/usr/bin/env node
import { spawn, spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { existsSync, mkdirSync, readFileSync, readdirSync, writeFileSync, writeSync, appendFileSync, copyFileSync, unlinkSync } from "node:fs";
import { basename, dirname, join, relative, resolve } from "node:path";
import process from "node:process";

const ROOT = resolve(dirname(new URL(import.meta.url).pathname), "../..");
const active = new Set();
const now = () => new Date().toISOString();
const sha = (value) => createHash("sha256").update(value).digest("hex");
const die = (message) => { writeSync(2, `ab-runner: ${message}\n`); process.exit(2); };

function parseArgs(argv) {
  const out = { pairs: 1, order: "alternate", output: join(ROOT, "tools/ab/results"), totalTimeout: 1800, toolTimeout: 300, agencyTimeout: 300, clientSilence: 180, transportSilence: 90 };
  for (let i = 0; i < argv.length; i++) {
    const key = argv[i];
    if (key === "--help") out.help = true;
    else if (["--scenario", "--pairs", "--order", "--output", "--model", "--agent", "--total-timeout", "--tool-timeout", "--agency-timeout", "--client-silence", "--transport-silence"].includes(key)) {
      if (!argv[i + 1]) die(`${key} requires a value`);
      const name = ({"--scenario":"scenario", "--pairs":"pairs", "--order":"order", "--output":"output", "--model":"model", "--agent":"agent", "--total-timeout":"totalTimeout", "--tool-timeout":"toolTimeout", "--agency-timeout":"agencyTimeout", "--client-silence":"clientSilence", "--transport-silence":"transportSilence"})[key];
      out[name] = ["pairs", "totalTimeout", "toolTimeout", "agencyTimeout", "clientSilence", "transportSilence"].includes(name) ? Number(argv[++i]) : argv[++i];
    } else die(`unknown argument: ${key}`);
  }
  return out;
}

function usage() {
  console.log(`Usage: node tools/ab/ab-runner.mjs --scenario <name|path> [options]

Required environment:
  OPENCODE_GO_KEY          direct credential
  ORIHSUS_GATEWAY_TOKEN    orihsus credential
  ORIHSUS_ENDPOINT         orihsus OpenAI-compatible base URL
  ORIHSUS_AUDIT_PATH or ORIHSUS_AUDIT_COMMAND (orihsus audit source)

Options: --pairs N --order alternate|AB|BA --output DIR --model ID --agent NAME
         --total-timeout SEC --tool-timeout SEC --agency-timeout SEC
         --client-silence SEC --transport-silence SEC`);
}

function walk(path, base = path) {
  return readdirSync(path, { withFileTypes: true }).sort((a,b) => a.name.localeCompare(b.name)).flatMap((entry) => {
    const full = join(path, entry.name);
    if (entry.name === ".git" || entry.name === "node_modules") return [];
    return entry.isDirectory() ? walk(full, base) : [[relative(base, full), readFileSync(full)]];
  });
}
function treeSha(path) {
  const h = createHash("sha256");
  for (const [name, body] of walk(path)) h.update(name).update("\0").update(body).update("\0");
  return h.digest("hex");
}
function copyTree(src, dst) {
  mkdirSync(dst, { recursive: true });
  for (const entry of readdirSync(src, { withFileTypes: true })) {
    const from = join(src, entry.name), to = join(dst, entry.name);
    if (entry.isDirectory()) copyTree(from, to); else copyFileSync(from, to);
  }
}
function run(cmd, args, opts = {}) {
  return spawnSync(cmd, args, { encoding: "utf8", ...opts });
}
function git(cwd, args) {
  const r = run("git", args, { cwd, env: { ...process.env, GIT_AUTHOR_NAME:"A/B Fixture", GIT_AUTHOR_EMAIL:"ab@example.invalid", GIT_COMMITTER_NAME:"A/B Fixture", GIT_COMMITTER_EMAIL:"ab@example.invalid", GIT_AUTHOR_DATE:"2000-01-01T00:00:00Z", GIT_COMMITTER_DATE:"2000-01-01T00:00:00Z" } });
  if (r.status !== 0) throw new Error(`git ${args.join(" ")} failed: ${r.stderr}`);
  return r.stdout.trim();
}
function fingerprint(secret) { return sha(secret).slice(0, 12); }
function redact(text, secrets) {
  let value = text;
  for (const secret of secrets) if (secret) value = value.split(secret).join("[REDACTED]");
  return value;
}

function validate(args) {
  if (!args.scenario) die("--scenario is required (see --help)");
  if (!Number.isInteger(args.pairs) || args.pairs < 1) die("--pairs must be a positive integer");
  if (!["alternate", "AB", "BA"].includes(args.order)) die("--order must be alternate, AB, or BA");
  for (const key of ["OPENCODE_GO_KEY", "ORIHSUS_GATEWAY_TOKEN", "ORIHSUS_ENDPOINT"]) if (!process.env[key]) die(`missing ${key}; credentials/endpoints are read from the environment and never stored`);
  if (!process.env.ORIHSUS_AUDIT_PATH && !process.env.ORIHSUS_AUDIT_COMMAND) die("missing orihsus audit source: set ORIHSUS_AUDIT_PATH or ORIHSUS_AUDIT_COMMAND");
  if (run(process.env.OPENCODE_BIN || "opencode", ["--version"]).status !== 0) die("opencode is not available");
}

function scenarioPath(input) {
  const direct = resolve(input);
  if (existsSync(direct)) return direct;
  return join(ROOT, "tools/ab/scenarios", input);
}
function loadScenario(input) {
  const path = scenarioPath(input), file = join(path, "scenario.json"), seed = join(path, "seed");
  if (!existsSync(file) || !existsSync(seed)) die(`invalid scenario ${input}: expected scenario.json and seed/`);
  const spec = JSON.parse(readFileSync(file, "utf8"));
  const actual = treeSha(seed);
  if (spec.initial_snapshot_sha256 !== actual) die(`scenario seed checksum mismatch: expected ${spec.initial_snapshot_sha256}, got ${actual}`);
  return { path, seed, spec, task: readFileSync(join(path, spec.task_file), "utf8") };
}

function configFor(group, key, endpoint, model) {
  const provider = group === "direct" ? "ab-direct" : "ab-pool";
  return { model: `${provider}/${model}`, provider: { [provider]: { npm: "@ai-sdk/openai-compatible", name: `A/B ${group}`, options: { baseURL: endpoint, apiKey: key }, models: { [model]: { name: model } } } }, permission: { edit: "allow", bash: "allow", read: "allow", glob: "allow", grep: "allow", list: "allow" } };
}
function jsonLines(path) {
  if (!existsSync(path)) return [];
  return readFileSync(path, "utf8").split(/\r?\n/).filter(Boolean).flatMap(line => { try { return [JSON.parse(line)]; } catch { return []; } });
}
function inspectClient(path) {
  const events = jsonLines(path); let sessionId = null, requestId = null, toolCount = 0, lastPart = null, lastTool = null, reasoning = 0, finalText = false;
  for (const e of events) {
    sessionId ||= e.sessionID || e.session_id || e.session?.id || e.part?.sessionID;
    requestId ||= e.requestID || e.request_id || e.messageID || e.part?.messageID;
    const type = String(e.type || e.part?.type || "");
    const ts = e.timestamp || e.time?.completed || e.time?.updated || e.part?.time?.end || null;
    if (type) lastPart = ts || lastPart;
    if (/reasoning|thinking/.test(type)) reasoning++;
    if (/tool/.test(type)) { toolCount++; if (/result|complete|completed/.test(type) || e.part?.state?.status === "completed") lastTool = ts || lastTool; }
    if (/text|finish|complete/.test(type)) finalText = true;
  }
  return { event_count: events.length, session_id: sessionId, request_id: requestId, tool_event_count: toolCount, reasoning_part_count: reasoning, final_text_observed: finalText, last_client_part_at: lastPart, last_tool_completed_at: lastTool };
}
function collectAudit(runDir, startedAt, client, secrets) {
  const raw = join(runDir, "pool-audit.raw.jsonl"), selected = join(runDir, "pool-audit.jsonl");
  let text = "";
  if (process.env.ORIHSUS_AUDIT_PATH) {
    if (!existsSync(process.env.ORIHSUS_AUDIT_PATH)) throw new Error(`audit path does not exist: ${process.env.ORIHSUS_AUDIT_PATH}`);
    text = readFileSync(process.env.ORIHSUS_AUDIT_PATH, "utf8");
  } else {
    const r = run("sh", ["-c", process.env.ORIHSUS_AUDIT_COMMAND], { env: process.env });
    if (r.status !== 0) throw new Error(`audit command failed: ${r.stderr}`);
    text = r.stdout;
  }
  text = redact(text, secrets); writeFileSync(raw, text, { mode: 0o600 });
  const start = Date.parse(startedAt) - 5000;
  const records = text.split(/\r?\n/).filter(Boolean).flatMap(line => { try { const v = JSON.parse(line); return [v]; } catch { return []; } }).filter(v => {
    const idMatch = (client.session_id && v.opencode_session_id === client.session_id) || (client.request_id && v.opencode_request_id === client.request_id);
    return idMatch || (!client.session_id && !client.request_id && Date.parse(v.timestamp) >= start);
  });
  writeFileSync(selected, records.map(v => JSON.stringify(v)).join("\n") + (records.length ? "\n" : ""));
  let last = null, events = 0, terminal = [];
  for (const record of records) for (const attempt of record.attempts || []) {
    events += Number(attempt.upstream_events || 0); terminal.push(attempt.terminal_reason);
    if (attempt.last_activity_offset_ms != null && record.timestamp) {
      const candidate = new Date(Date.parse(record.timestamp) - Number(record.latency_ms || 0) + Number(attempt.last_activity_offset_ms)).toISOString();
      if (!last || candidate > last) last = candidate;
    }
  }
  return { record_count: records.length, upstream_event_count: events, terminal_reasons: terminal, last_upstream_event_at: last };
}
function grade({ runnerError, timedOut, audit, client, acceptance }) {
  if (runnerError) return { level: 1, label: "infrastructure_or_runner_failure" };
  if (audit && audit.upstream_event_count === 0 && client.event_count === 0) return { level: 2, label: "transport_silent" };
  if (audit && audit.upstream_event_count > 0 && client.event_count === 0) return { level: 3, label: "client_silent_upstream_active" };
  if (timedOut && client.reasoning_part_count > 0 && client.tool_event_count === 0) return { level: 4, label: "active_prolonged_reasoning" };
  if (timedOut) return { level: 7, label: "total_deadline_exceeded" };
  if (acceptance.exit_code === 0) return { level: 9, label: "mechanical_acceptance_passed" };
  if (client.tool_event_count > 0) return { level: 5, label: "tool_loop_active" };
  return { level: 1, label: "infrastructure_or_runner_failure" };
}

async function execute(group, pair, sequence, suite, scenario, args) {
  const runDir = join(suite, `pair-${String(pair).padStart(2,"0")}-${sequence}-${group}`);
  const work = join(runDir, "worktree"), xdg = { data:join(runDir,"xdg/data"), cache:join(runDir,"xdg/cache"), config:join(runDir,"xdg/config"), state:join(runDir,"xdg/state") };
  mkdirSync(runDir, { recursive:true }); copyTree(scenario.seed, work); Object.values(xdg).forEach(p => mkdirSync(p, {recursive:true}));
  git(work, ["init", "-q"]); git(work, ["add", "."]); git(work, ["commit", "-q", "-m", "fixture snapshot"]);
  const initialCommit = git(work, ["rev-parse", "HEAD"]), initialTree = git(work, ["rev-parse", "HEAD^{tree}"]), startedAt = now();
  const key = group === "direct" ? process.env.OPENCODE_GO_KEY : process.env.ORIHSUS_GATEWAY_TOKEN;
  const endpoint = group === "direct" ? (process.env.OPENCODE_GO_ENDPOINT || "https://opencode.ai/zen/go/v1") : process.env.ORIHSUS_ENDPOINT;
  const model = args.model || scenario.spec.model || "deepseek-v4-flash";
  const clientConfig = configFor(group,key,endpoint,model);
  const env = { ...process.env, XDG_DATA_HOME:xdg.data, XDG_CACHE_HOME:xdg.cache, XDG_CONFIG_HOME:xdg.config, XDG_STATE_HOME:xdg.state, OPENCODE_CONFIG_CONTENT:JSON.stringify(clientConfig) };
  const stdout = join(runDir,"client.jsonl"), stderr = join(runDir,"client.stderr.log"), secrets = [process.env.OPENCODE_GO_KEY, process.env.ORIHSUS_GATEWAY_TOKEN];
  const manifest = { schema_version:1, scenario:scenario.spec.id, task_sha256:sha(scenario.task), seed_sha256:treeSha(scenario.seed), initial_commit:initialCommit, repo:{ source:"versioned fixture", commit:initialCommit, tree:initialTree, dirty:false }, group, pair, sequence, started_at:startedAt, client:{ binary:process.env.OPENCODE_BIN||"opencode", version:run(process.env.OPENCODE_BIN||"opencode",["--version"]).stdout.trim(), command:["run","--pure","--auto","--format","json"], format:"json", agent:args.agent||scenario.spec.agent||"build", pure:true, approval_policy:"auto" }, model, model_config:{ provider_package:"@ai-sdk/openai-compatible", catalog_sha256:sha(JSON.stringify({model,provider:group})) }, endpoint:{ kind:group, base_url:endpoint }, task_and_system_prompt:{ task_file:scenario.spec.task_file, task_sha256:sha(scenario.task), additional_system_prompt_sha256:null }, tools:["read","glob","grep","list","edit","bash"], credential_fingerprint:fingerprint(key), timeouts_seconds:{ transport_silence:args.transportSilence, client_silence:args.clientSilence, agency:args.agencyTimeout, tool_execution:args.toolTimeout, total:args.totalTimeout }, acceptance_command:scenario.spec.acceptance.command, review_rubric:scenario.spec.review_rubric, host:{ platform:process.platform, arch:process.arch, node:process.version }, pool_deployment_commit:process.env.ORIHSUS_DEPLOYMENT_COMMIT||null };
  writeFileSync(join(runDir,"manifest.json"), JSON.stringify(manifest,null,2)+"\n", {mode:0o600});
  let timedOut=false, runnerError=null, exitCode=null;
  const child = spawn(process.env.OPENCODE_BIN||"opencode", ["run","--pure","--auto","--format","json","--print-logs","--model",`${group === "direct" ? "ab-direct":"ab-pool"}/${model}`,"--agent",args.agent||scenario.spec.agent||"build","--dir",work,scenario.task], { cwd:work, env, detached:true, stdio:["ignore","pipe","pipe"] });
  active.add(child.pid); child.stdout.on("data",d=>appendFileSync(stdout,redact(d.toString(),secrets))); child.stderr.on("data",d=>appendFileSync(stderr,redact(d.toString(),secrets)));
  const timer=setTimeout(()=>{ timedOut=true; try { process.kill(-child.pid,"SIGTERM"); } catch {} setTimeout(()=>{try{process.kill(-child.pid,"SIGKILL");}catch{}},5000).unref(); },args.totalTimeout*1000);
  await new Promise(resolve=>{child.on("error",e=>{runnerError=e.message;resolve();}); child.on("exit",code=>{exitCode=code;resolve();});}); clearTimeout(timer); active.delete(child.pid);
  await new Promise(r=>setTimeout(r,300));
  try { process.kill(-child.pid,0); runnerError ||= "child process group remained after cleanup"; try{process.kill(-child.pid,"SIGKILL");}catch{} } catch {}
  const client=inspectClient(stdout); let audit=null;
  manifest.session_id=client.session_id; manifest.request_id=client.request_id;
  writeFileSync(join(runDir,"manifest.json"), JSON.stringify(manifest,null,2)+"\n", {mode:0o600});
  if(group==="pool") try{audit=collectAudit(runDir,startedAt,client,secrets);}catch(e){runnerError ||= e.message;}
  const accept=run("sh",["-c",scenario.spec.acceptance.command],{cwd:work,timeout:args.toolTimeout*1000});
  const acceptance={ exit_code:accept.status, signal:accept.signal, stdout:redact(accept.stdout||"",secrets), stderr:redact(accept.stderr||"",secrets) };
  writeFileSync(join(runDir,"acceptance.json"),JSON.stringify(acceptance,null,2)+"\n");
  writeFileSync(join(runDir,"worktree.diff"),git(work,["diff","--binary","HEAD"]));
  writeFileSync(join(runDir,"worktree.status"),git(work,["status","--short"])+"\n");
  const result={ finished_at:now(), wall_time_ms:Date.now()-Date.parse(startedAt), client_exit_code:exitCode, timed_out:timedOut, runner_error:runnerError, clocks:{ last_upstream_event_at:audit?.last_upstream_event_at||null, last_client_part_at:client.last_client_part_at, last_tool_completed_at:client.last_tool_completed_at }, client, audit, acceptance, grade:grade({runnerError,timedOut,audit,client,acceptance}) };
  writeFileSync(join(runDir,"result.json"),JSON.stringify(result,null,2)+"\n");
  for(const secret of secrets) if(secret) for(const [name,body] of walk(runDir)) if(body.includes(Buffer.from(secret))) { unlinkSync(join(runDir,name)); throw new Error(`secret detected and removed from artifact ${name}`); }
  return { group, pair, run_dir:relative(suite,runDir), grade:result.grade, acceptance_exit_code:acceptance.exit_code, wall_time_ms:result.wall_time_ms };
}

async function cleanup() { for(const pid of active){try{process.kill(-pid,"SIGTERM");}catch{}} await new Promise(r=>setTimeout(r,300)); for(const pid of active){try{process.kill(-pid,"SIGKILL");}catch{}} }
process.on("SIGINT",()=>cleanup().finally(()=>process.exit(130))); process.on("SIGTERM",()=>cleanup().finally(()=>process.exit(143)));

const args=parseArgs(process.argv.slice(2)); if(args.help){usage();process.exit(0);} validate(args); const scenario=loadScenario(args.scenario);
const suite=resolve(args.output,`${new Date().toISOString().replace(/[:.]/g,"-")}-${scenario.spec.id}`); mkdirSync(suite,{recursive:true});
const summary=[];
try { for(let pair=1;pair<=args.pairs;pair++){ const order=args.order==="alternate"?(pair%2?"AB":"BA"):args.order; for(let i=0;i<2;i++) summary.push(await execute(order[i]==="A"?"direct":"pool",pair,i+1,suite,scenario,args)); } }
finally { await cleanup(); }
writeFileSync(join(suite,"summary.json"),JSON.stringify({scenario:scenario.spec.id,pairs:args.pairs,runs:summary},null,2)+"\n");
console.log(suite);
