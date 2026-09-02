#!/usr/bin/env python3
"""Guided demo runner for the assignment's Loom video.

One command per recorded segment, so you don't have to remember or retype
raw curl/node commands while recording. This is a *demo aid*, not a
correctness check — see scenarios/validate.py for that. No pass/fail here,
just clean, repeatable output.

Usage:
  python3 scripts/video-demo.py reset      # pre-recording state reset
  python3 scripts/video-demo.py load       # segment 1: load burst
  python3 scripts/video-demo.py throttle   # segment 2: live limit change
  python3 scripts/video-demo.py batch      # segment 3: async batch + callback
  python3 scripts/video-demo.py            # list available segments

Run ./scripts/demo-logs.sh in a second terminal pane for the live log tail
shown alongside each segment.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
import urllib.request
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
LOADGEN = str(REPO_ROOT / "tools/loadgen/index.js")

BASE_URL = "http://localhost:8081"
CALLBACK_RECEIVER_URL = "http://localhost:9000"
# As seen from inside the docker network (what the system uses to reach webhook-mock)
CALLBACK_URL_FOR_SYSTEM = "http://webhook-mock:9000/callback"


def log(msg: str) -> None:
    print(f"[demo {datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def log_step(msg: str) -> None:
    log(f"=== {msg} ===")


def api(method: str, path: str, body: dict | None = None, base: str = BASE_URL, timeout: int = 30):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        base + path,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"} if data else {},
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.load(resp)


def wait_for_gateway() -> None:
    for _ in range(30):
        try:
            urllib.request.urlopen(BASE_URL + "/actuator/health", timeout=3)
            return
        except Exception:
            time.sleep(1)
    log("WARNING: gateway did not respond to health check — continuing anyway")


def run_loadgen(*extra_args: str, background: bool = False):
    cmd = ["node", LOADGEN, "--url", BASE_URL, *extra_args]
    if background:
        return subprocess.Popen(cmd)
    subprocess.run(cmd, check=True)
    return None


def cmd_reset(_args) -> None:
    log_step("Reset demo state")
    log("truncating requests + batches in postgres")
    subprocess.run(
        [
            "docker", "compose", "exec", "-T", "postgres",
            "psql", "-U", "inference", "-d", "inference",
            "-c", "TRUNCATE requests, batches",
        ],
        check=True,
        cwd=REPO_ROOT,
    )
    log("resetting webhook-mock")
    api("POST", "/reset", base=CALLBACK_RECEIVER_URL)
    log("ready to record")


def cmd_load(_args) -> None:
    log_step("Segment 1: processing under load")
    wait_for_gateway()
    log("running loadgen: ~1,000 req/s for 15s across model-a/model-b")
    run_loadgen(
        "--rate", "1000",
        "--duration", "15",
        "--models", "model-a:0.6,model-b:0.4",
        "--tokens", "1000",
        "--track-sample", "500",
    )
    log("done")


def cmd_throttle(_args) -> None:
    log_step("Segment 2: live limit change")
    wait_for_gateway()

    log("current model limits:")
    print(json.dumps(api("GET", "/v1/admin/models"), indent=2))

    log("starting background load: 900 req/s for 25s on model-a")
    proc = run_loadgen(
        "--rate", "900",
        "--duration", "25",
        "--models", "model-a:1.0",
        "--tokens", "1000",
        "--track-sample", "300",
        background=True,
    )

    try:
        time.sleep(3)
        log("throttling model-a to 3,000 RPM — watch the log pane for 'rate limited' lines")
        api("PUT", "/v1/admin/models/model-a", {"rpmLimit": 3000, "tpmLimit": 60_000_000})

        time.sleep(10)
        log("restoring model-a to 30,000 RPM")
        api("PUT", "/v1/admin/models/model-a", {"rpmLimit": 30_000, "tpmLimit": 60_000_000})
    finally:
        log("waiting for background load to finish...")
        proc.wait()

    log("done")


def cmd_batch(_args) -> None:
    log_step("Segment 3: async batch + callback")
    wait_for_gateway()

    run_id = int(time.time())
    size = 300
    requests = [
        {
            "requestId": f"demo-{run_id}-{i}",
            "model": "model-a" if i % 2 == 0 else "model-b",
            "estimatedTokens": 1000,
            "payload": {"prompt": f"demo request {i}"},
        }
        for i in range(size)
    ]
    body = {"callbackUrl": CALLBACK_URL_FOR_SYSTEM, "requests": requests}

    log(f"submitting batch of {size} requests (unique ids — safe to rerun this command anytime)")
    t0 = time.time()
    ack = api("POST", "/v1/batches", body)
    ack_ms = (time.time() - t0) * 1000
    batch_id = ack["batchId"]
    log(f"ack in {ack_ms:.0f}ms — batchId={batch_id}")

    log("waiting for batch to complete...")
    status = None
    for _ in range(60):
        status = api("GET", f"/v1/batches/{batch_id}")
        if status["status"] == "COMPLETED":
            break
        time.sleep(1)
    print(json.dumps(status, indent=2))

    log("waiting for callback delivery...")
    for _ in range(30):
        status = api("GET", f"/v1/batches/{batch_id}")
        if status.get("callbackStatus") == "DELIVERED":
            break
        time.sleep(1)

    log("callback received by webhook-mock:")
    received = api("GET", "/received", base=CALLBACK_RECEIVER_URL)
    if received["received"]:
        print(json.dumps(received["received"][-1]["body"], indent=2))
    else:
        log("WARNING: no callback recorded yet — check callbackStatus above")

    log(f"done — batchId={batch_id}")


CASES = {
    "reset": (cmd_reset, "Truncate DB + reset webhook-mock before recording"),
    "load": (cmd_load, "Segment 1: ~1,000 req/s burst across both models"),
    "throttle": (cmd_throttle, "Segment 2: live model-a limit change while under load"),
    "batch": (cmd_batch, "Segment 3: async batch submission + callback"),
}


def main() -> int:
    parser = argparse.ArgumentParser(description="Guided demo runner for the assignment video")
    parser.add_argument("case", nargs="?", choices=list(CASES.keys()))
    args = parser.parse_args()

    if not args.case:
        print("Usage: python3 scripts/video-demo.py <case>\n")
        print("Available cases:")
        for name, (_, desc) in CASES.items():
            print(f"  {name:10s} {desc}")
        print("\nFor the live log pane during recording, run in another terminal:")
        print("  ./scripts/demo-logs.sh")
        return 0

    fn, _ = CASES[args.case]
    fn(args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
