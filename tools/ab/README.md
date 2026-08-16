# Agent programming A/B runner

This directory implements the first real A/B loop described by the agent-programming methodology: the same versioned task and seed are run through the official OpenCode Go path (`direct`) and orihsus (`pool`), with isolated client state and separately preserved evidence.

## Prerequisites and running

Requirements are Node.js 20+, Git, OpenCode, and `sh`. Credentials are read only from the environment:

```sh
export OPENCODE_GO_KEY='...'
export ORIHSUS_GATEWAY_TOKEN='...'
export ORIHSUS_ENDPOINT='https://orihsus.example/v1'

# Choose one audit source. A local file is simplest:
export ORIHSUS_AUDIT_PATH='/var/log/orihsus/audit.jsonl'
# Or print the relevant audit log to stdout, for example from a remote host:
# export ORIHSUS_AUDIT_COMMAND='ssh orihsus-host sudo cat /var/log/orihsus/audit.jsonl'

node tools/ab/ab-runner.mjs --scenario controlled-tool-loop --pairs 3
```

`OPENCODE_GO_ENDPOINT` defaults to `https://opencode.ai/zen/go/v1`. The orihsus endpoint has no default so accidentally testing the wrong deployment is impossible. `OPENCODE_BIN`, `ORIHSUS_DEPLOYMENT_COMMIT`, `--model`, `--agent`, `--output`, and the five timeout options can be used to pin the remaining manifest fields. `--order alternate` is the default: pair 1 runs AB, pair 2 BA, and so on. `--order AB` and `--order BA` force an order. The runner validates both credentials and the audit source before creating a suite. Missing keys fail explicitly:

```text
ab-runner: missing OPENCODE_GO_KEY; credentials/endpoints are read from the environment and never stored
```

Do not put credentials in task text, scenario JSON, shell history, or an audit command. The OpenCode provider configuration containing a key exists only in the child environment. Every artifact is scanned for both raw credentials after each run.

## Isolation, cleanup, and evidence

Each member of each pair gets its own worktree and `XDG_DATA_HOME`, `XDG_CACHE_HOME`, `XDG_CONFIG_HOME`, and `XDG_STATE_HOME`. This also isolates OpenCode's SQLite/session database. The seed is copied and committed with fixed Git identity and timestamps, making `initial_commit` reproducible. The runner starts OpenCode in a process group, sends `SIGTERM` at the total deadline or on runner interruption, follows with `SIGKILL`, and verifies that the process group is gone before grading.

Artifacts are written under `tools/ab/results/<timestamp>-<scenario>/` by default (the directory is gitignored). Each run contains:

- `manifest.json`: task/seed hashes, deterministic initial commit, client version and arguments, model, endpoint kind and URL, timeouts, host/runtime, acceptance command, rubric, deployment commit, and only a 12-character credential fingerprint;
- `client.jsonl` and `client.stderr.log`: what the client exposed;
- `xdg/`: isolated client state, including its SQLite data;
- `pool-audit.raw.jsonl` and `pool-audit.jsonl`: redacted source capture and records correlated by OpenCode session/request ID (timestamp fallback is used only when the client emitted neither ID);
- `worktree.diff`, `worktree.status`, and `acceptance.json`: the actual delivery and mechanical oracle;
- `result.json`: three clocks, observation summaries, grade, and errors.

The audit command is executed only after a pool run. It must print JSONL in Orihsus' audit schema. For a large production log, make the command select a safe time window server-side. Full reasoning is not copied into the manifest or result summary; raw client JSONL may contain model output and should follow the environment's retention policy.

## What each layer measures

The client JSONL/stderr layer answers what the operator could see: completed parts, tool events, final text, errors, and exit status. The isolated SQLite/session layer preserves part start/end, tool state, compact/session state, and is the source for deeper diagnosis when JSONL aggregates a long part. Pool audit is authoritative for response/first-event timing, upstream event counts, attempts, fingerprints, failover, and terminal reasons.

`result.json` keeps the clocks separate:

- `last_upstream_event_at`: transport activity reconstructed from audit attempt offsets;
- `last_client_part_at`: last timestamp exposed in a JSONL part, when the client supplies one;
- `last_tool_completed_at`: last completed tool timestamp exposed by the client.

The configured transport-silence, client-silence, agency, per-tool, and total deadlines are recorded independently. In this first phase only the total deadline and acceptance-command timeout actively terminate processes; the other three are observational thresholds because upstream audit is normally collected after the request. They must not be collapsed into one “silent” timeout.

## Grades and interpretation

Grades follow the methodology's ten-level ladder: 1 infrastructure/runner failure, 2 transport silent, 3 client silent while upstream active, 4 prolonged active reasoning, 5 tool loop active, 6 tool failure/retry loop, 7 total deadline, 8 implementation complete, 9 mechanical acceptance passed, and 10 independent review approved. Automation assigns only grades it can prove; grade 10 is intentionally reserved for a reviewer verdict added after inspecting the rubric. Grades 6 and 8 may likewise require review of the trace/diff when client event schemas do not expose a reliable signal.

Compare paired runs in this order: transport invariants, sustained tool loop, mechanical acceptance, independent review, then distributions over at least three (diagnosis) or five (comparison) pairs. A single pair is a reproducer, not a performance conclusion. Infrastructure-invalid runs never count against either model path.

## P0 scenario matrix

| Scenario | Layer | Risk exercised | Mechanical oracle | Review emphasis |
| --- | --- | --- | --- | --- |
| `controlled-tool-loop` | L2 | multi-round tool/result feedback and failure-driven repair | three parser tests pass | strict parsing, valid sum/skips, tests preserved |
| `repository-bugfix` | L3 | search, local comprehension, regression avoidance | existing router suite goes red then green | escaping literals without breaking parameters |
| `usine-bootstrap` | L3 | open-ended planning and greenfield delivery | required structure plus complete Node test suite | concurrency semantics, failure isolation, coherent docs/tests |

Each scenario versions `task.md`, `scenario.json`, and `seed/`. The seed checksum is verified before every suite and the deterministic initial Git commit is written to each manifest. Run runner contract tests (no real credentials or network required) with:

```sh
node tools/ab/test-runner.mjs
```
