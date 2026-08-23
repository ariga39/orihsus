#!/usr/bin/env python3
"""Exercise real non-streaming and SSE coding requests without logging bodies."""
import http.client
import json
import os
import sys
import time
from pathlib import Path

credentials_path, audit_path, pid_text = sys.argv[1:]
credentials = json.loads(Path(credentials_path).read_text())
audit_file = Path(audit_path)
pid = int(pid_text)

CODE = r'''
export class AsyncWorkQueue {
  constructor(limit = 3) {
    this.limit = limit;
    this.active = 0;
    this.pending = [];
    this.closed = false;
  }
  enqueue(task, signal) {
    if (this.closed) return Promise.reject(new Error("closed"));
    return new Promise((resolve, reject) => {
      const item = { task, resolve, reject, signal };
      if (signal?.aborted) return reject(signal.reason);
      signal?.addEventListener("abort", () => reject(signal.reason));
      this.pending.push(item);
      this.drain();
    });
  }
  drain() {
    while (this.active < this.limit && this.pending.length) {
      const item = this.pending.shift();
      this.active++;
      Promise.resolve().then(item.task).then(item.resolve, item.reject).finally(() => {
        this.active--;
        this.drain();
      });
    }
  }
  close() {
    this.closed = true;
    for (const item of this.pending) item.reject(new Error("closed"));
    this.pending = [];
  }
}
'''

def rss_kib():
    for line in Path(f"/proc/{pid}/status").read_text().splitlines():
        if line.startswith("VmRSS:"):
            return int(line.split()[1])
    raise AssertionError("VmRSS unavailable")

def request(token, payload, stream=False):
    connection = http.client.HTTPConnection("127.0.0.1", 18082, timeout=240)
    connection.request(
        "POST", "/v1/chat/completions", json.dumps(payload).encode(),
        {"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    )
    response = connection.getresponse()
    if response.status != 200:
        response.read()
        raise AssertionError(f"upstream request returned HTTP {response.status}")
    if not stream:
        value = json.loads(response.read())
        connection.close()
        message = value["choices"][0]["message"]
        content = "".join(
            message.get(field) or ""
            for field in ("content", "reasoning_content", "reasoning")
            if isinstance(message.get(field), str)
        )
        assert len(content) >= 40, "non-stream response text was empty or truncated"
        assert isinstance(value.get("usage"), dict)
        return content

    assert "text/event-stream" in (response.getheader("content-type") or "")
    data_events = 0
    done = False
    terminal_event = False
    final_usage = None
    content_parts = []
    while True:
        line = response.readline()
        if not line:
            break
        assert line.endswith(b"\n"), "truncated SSE line"
        if line in (b"\n", b"\r\n") or line.startswith(b":"):
            continue
        assert line.startswith(b"data:"), "unexpected or corrupted SSE field"
        payload_text = line[5:].strip()
        if payload_text == b"[DONE]":
            done = True
            continue
        event = json.loads(payload_text)
        data_events += 1
        if isinstance(event.get("usage"), dict):
            final_usage = event["usage"]
        if event.get("type") in ("response.completed", "message_stop"):
            terminal_event = True
        if event.get("finish_reason") is not None or event.get("stop_reason") is not None:
            terminal_event = True
        choices = event.get("choices") or []
        if choices:
            if choices[0].get("finish_reason") is not None or choices[0].get("stop_reason") is not None:
                terminal_event = True
            delta = choices[0].get("delta", {})
            for field in ("content", "reasoning_content", "reasoning"):
                piece = delta.get(field)
                if isinstance(piece, str):
                    content_parts.append(piece)
    connection.close()
    if not (done or terminal_event):
        completion = None if final_usage is None else final_usage.get("completion_tokens")
        raise AssertionError(
            "SSE stream reached clean EOF without a terminal event "
            f"(usage_seen={final_usage is not None}, completion_tokens={completion}, "
            f"max_tokens={payload.get('max_tokens')})"
        )
    assert data_events >= 2, "SSE stream did not contain a meaningful event sequence"
    assert isinstance(final_usage, dict), "SSE stream ended without its requested usage event"
    content = "".join(content_parts)
    assert len(content) >= 40, "SSE content was empty or truncated"
    return content

def models(token):
    connection = http.client.HTTPConnection("127.0.0.1", 18082, timeout=10)
    connection.request("GET", "/v1/models", headers={"Authorization": f"Bearer {token}"})
    response = connection.getresponse()
    body = response.read()
    connection.close()
    assert response.status == 200
    assert any(item["id"] == "ox-alpha-free" for item in json.loads(body)["data"])

for token in credentials.values():
    models(token)

before_rss = rss_kib()
primary = credentials["e2e-primary"]
secondary = credentials["e2e-secondary"]
review = request(primary, {
    "model": "ox-alpha-free", "stream": False, "max_tokens": 1200,
    "messages": [{"role": "user", "content": "Review this production queue implementation for concurrency, cancellation, listener leaks, and close races. Give a concrete patch strategy and at least six focused tests.\n\n" + CODE}],
})
request(secondary, {
    "model": "ox-alpha-free", "stream": True, "max_tokens": 1600,
    "stream_options": {"include_usage": True},
    "messages": [
        {"role": "user", "content": "Review this asynchronous queue implementation:\n\n" + CODE},
        {"role": "assistant", "content": review},
        {"role": "user", "content": "Now produce a corrected implementation and explain why every state transition is race-safe. Include executable node:test cases, but keep the complete answer under 900 tokens."},
    ],
}, stream=True)

# Repeated substantial requests exercise connection reuse/pooling, audit writes,
# identity attribution, and process stability rather than a one-shot smoke test.
for index in range(4):
    token = primary if index % 2 == 0 else secondary
    streaming = index % 2 == 1
    payload = {
        "model": "ox-alpha-free", "stream": index % 2 == 1, "max_tokens": 700,
        "messages": [{"role": "user", "content": f"Pass {index + 1}: identify one subtle correctness bug in this queue and write a regression test that distinguishes the fix from the original. Be precise and keep the answer under 350 tokens.\n\n" + CODE}],
    }
    if streaming:
        payload["stream_options"] = {"include_usage": True}
    request(token, payload, stream=streaming)

after_rss = rss_kib()
assert after_rss < 512 * 1024, f"gateway RSS too high: {after_rss} KiB"
assert after_rss - before_rss < 256 * 1024, f"gateway RSS grew excessively: {after_rss - before_rss} KiB"

deadline = time.time() + 10
records = []
while time.time() < deadline:
    if audit_file.exists():
        records = [json.loads(line) for line in audit_file.read_text().splitlines() if line]
    successful = [r for r in records if r.get("status") == 200 and r.get("model") == "ox-alpha-free"]
    if len(successful) >= 6:
        break
    time.sleep(0.1)
assert len(successful) >= 6, "audit did not persist every real coding request"
assert {r.get("gateway_key") for r in successful} >= {"e2e-primary", "e2e-secondary"}
for record in successful:
    for field in ("input_tokens", "cached_tokens", "uncached_tokens", "output_tokens"):
        assert isinstance(record.get(field), int) and record[field] >= 0, f"missing audit usage field: {field}"
    assert record["uncached_tokens"] == record["input_tokens"] - record["cached_tokens"]

print(f"deployment-gate verification passed: {len(successful)} coding requests; RSS {before_rss}->{after_rss} KiB")
