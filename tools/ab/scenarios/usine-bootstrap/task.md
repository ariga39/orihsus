Bootstrap a small production-minded job pipeline named Usine in this empty repository. This is deliberately open-ended: plan the structure, then implement it.

Requirements:

- Use dependency-free Node.js ES modules.
- Expose a `createPipeline({ concurrency })` API from `src/pipeline.js` with `submit(job)`, `onResult(listener)`, and asynchronous `close()` methods.
- A job has a unique string `id` and an async `run()` function. Reject duplicate IDs. Never run more than `concurrency` jobs simultaneously. One failed job must produce a result and must not stop queued jobs.
- Add meaningful automated tests for concurrency, duplicate IDs, failure isolation, and graceful close.
- Add a concise README explaining architecture, API, failure behavior, and how to run tests.
- Provide `npm test`; use only built-in Node modules.

Finish by running the full test suite. Prefer a small coherent design over scaffolding or placeholders.
