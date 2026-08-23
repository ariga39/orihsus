# Real upstream deployment gate

This opt-in deployment gate compiles the current checkout, launches a loopback-only
orihsus instance, and runs six substantial coding requests against the real
OpenCode `ox-alpha-free` model. It verifies both named gateway credentials,
non-streaming and SSE responses, complete SSE framing and terminal events,
per-request usage audit fields, repeated-request stability, diagnostics, and
bounded RSS growth.

Prerequisites: `bash`, Rust/Cargo, Python 3 with PyYAML, `curl`, direct network
access to `opencode.ai`, and a dedicated real-upstream test key. Supply the key
through exactly one of `ORIHSUS_E2E_KEY` or `ORIHSUS_E2E_KEY_FILE`. The file
form is preferred on shared systems because it does not place the key in the
environment inherited by the invoking shell.

Run only as a deployment gate, never from CI:

```bash
ORIHSUS_E2E_KEY_FILE=/run/secrets/orihsus-e2e-key tools/e2e-real/run.sh
```

This gate is deliberately absent from the default test suite. An opt-in CI job
can inject a repository/environment secret without coupling to production, for
example in GitHub Actions:

```yaml
- name: Real upstream deployment gate
  env:
    ORIHSUS_E2E_KEY: ${{ secrets.ORIHSUS_E2E_KEY }}
  run: tools/e2e-real/run.sh
```

The script never prints credentials or model response bodies. It copies the
injected key into a mode-0600 `/tmp` directory, derives two random test
identities, connects directly to the fixed real upstream, and removes the
config, credentials, audit, logs, process, and `CARGO_TARGET_DIR` via an exit
trap whether the test passes or fails.
