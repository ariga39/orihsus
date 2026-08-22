#!/usr/bin/env python3
"""Create a secret local deployment-gate config without printing credentials."""
import json
import secrets
import sys
from pathlib import Path

import yaml

source, destination, credentials, audit_path, usage_dir = map(Path, sys.argv[1:])
key = source.read_text().rstrip("\r\n")
if not key or "\n" in key or "\r" in key or key != key.strip():
    raise SystemExit("deployment-gate: injected upstream key must be one non-blank line")

identities = {
    "e2e-primary": secrets.token_urlsafe(32),
    "e2e-secondary": secrets.token_urlsafe(32),
}
config = {
    "listen": {"host": "127.0.0.1", "port": 18082},
    "gateway_keys": [{"name": name, "token": token} for name, token in identities.items()],
    "keys": [key],
    "models": ["ox-alpha-free"],
    "model_sync": {"enabled": False},
    "limits": {"max_concurrency": 8, "max_queue": 16, "max_body_bytes": 1048576},
    "usage": {"poll_interval_seconds": 3600},
    "usage_history_dir": str(usage_dir),
    "audit": {"path": str(audit_path), "queue_capacity": 128},
    "server": {
        "upstream_response_header_timeout_seconds": 180,
        "first_event_timeout_seconds": 180,
        "inter_event_timeout_seconds": 180,
    },
}
destination.write_text(yaml.safe_dump(config, sort_keys=False))
credentials.write_text(json.dumps(identities))
