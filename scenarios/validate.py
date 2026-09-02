#!/usr/bin/env python3
"""Run assignment validation scenarios against a live stack."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
import urllib.error
import urllib.request
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent

BASE_URL = "http://localhost:8080"
CALLBACK_URL = "http://webhook-mock:9000/callback"
TOKENS_PER_REQUEST = 1000
PG_CONTAINER = "high-throughput-inference-postgres-1"


def log(msg: str) -> None:
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[validate {ts}] {msg}", flush=True)


def log_step(msg: str) -> None:
    log(f"=== {msg} ===")


def log_metrics(title: str, metrics: dict[str, Any]) -> None:
    log(title)
    for key, value in metrics.items():
        log(f"  {key}: {value}")


@dataclass
class ScenarioResult:
    name: str
    passed: bool
    details: dict[str, Any] = field(default_factory=dict)


def api(method: str, path: str, body: dict | None = None, timeout: int = 30) -> Any:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        BASE_URL + path,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"} if data else {},
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.load(resp)


def psql(sql: str) -> str:
    cmd = [
        "docker", "exec", PG_CONTAINER,
        "psql", "-U", "inference", "-d", "inference", "-t", "-A", "-c", sql,
    ]
    return subprocess.check_output(cmd, text=True).strip()


def set_model_limits(name: str, rpm: int, tpm: int) -> None:
    log(f"PUT /v1/admin/models/{name} → rpm={rpm:,} tpm={tpm:,}")
    api("PUT", f"/v1/admin/models/{name}", {"rpmLimit": rpm, "tpmLimit": tpm})


def reset_callback_mock() -> None:
    log("resetting webhook-mock (POST /reset)")
    req = urllib.request.Request("http://localhost:9000/reset", method="POST")
    with urllib.request.urlopen(req, timeout=5):
        pass


def reset_pipeline_state() -> None:
    """Clear queued work so scenarios start from a clean slate."""
    log("truncating requests + batches in postgres")
    psql("TRUNCATE requests, batches")
    reset_callback_mock()
    log("pipeline state reset complete")


def submit_request(request_id: str, model: str, tokens: int = TOKENS_PER_REQUEST) -> bool:
    try:
        api("POST", "/v1/inference", {
            "requestId": request_id,
            "model": model,
            "estimatedTokens": tokens,
            "payload": {"id": request_id},
        })
        return True
    except Exception:
        return False


def submit_load(prefix: str, model: str, count: int, workers: int = 100) -> int:
    submitted = 0

    def one(i: int) -> bool:
        return submit_request(f"{prefix}-{i}", model)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(one, i) for i in range(count)]
        for f in as_completed(futures):
            if f.result():
                submitted += 1
    return submitted


def submit_load_duration(prefix: str, model: str, rate: int, duration_sec: int) -> int:
    submitted = 0
    start = time.time()
    end = start + duration_sec
    interval = 1.0 / rate
    i = 0
    pool = ThreadPoolExecutor(max_workers=200)
    last_log = start

    log(f"loadgen start model={model} target={rate}/s duration={duration_sec}s prefix={prefix}")
    while time.time() < end:
        tick = time.time()
        batch = max(1, rate // 10)
        for _ in range(batch):
            pool.submit(submit_request, f"{prefix}-{i}", model)
            i += 1
            submitted += 1
        elapsed = time.time() - tick
        sleep = max(0, (batch * interval) - elapsed)
        time.sleep(sleep)
        now = time.time()
        if now - last_log >= 30:
            elapsed_sec = int(now - start)
            log(f"loadgen progress model={model} submitted={submitted:,} elapsed={elapsed_sec}s")
            last_log = now

    pool.shutdown(wait=False)
    log(f"loadgen done model={model} submitted={submitted:,} in {int(time.time() - start)}s")
    return submitted


def fetch_completions(
    since_epoch: float,
    *,
    states: tuple[str, ...] | None = None,
    prefix: str | None = None,
) -> list[tuple[float, str, int]]:
    since = datetime.fromtimestamp(since_epoch, tz=timezone.utc).isoformat()
    clauses = [
        "completed_at IS NOT NULL",
        f"submitted_at >= '{since}'",
    ]
    if states:
        quoted = ", ".join(f"'{state}'" for state in states)
        clauses.append(f"state IN ({quoted})")
    if prefix:
        clauses.append(f"request_id LIKE '{prefix}%'")
    where = " AND ".join(clauses)
    rows = psql(
        f"SELECT extract(epoch from completed_at), model, estimated_tokens "
        f"FROM requests WHERE {where} "
        f"ORDER BY completed_at"
    )
    if not rows:
        return []
    result = []
    for line in rows.splitlines():
        if not line.strip():
            continue
        parts = line.split("|")
        if len(parts) != 3:
            continue
        result.append((float(parts[0]), parts[1], int(parts[2])))
    return result


def max_in_sliding_window(events: list[float], window_sec: int = 60) -> int:
    if not events:
        return 0
    events = sorted(events)
    max_count = 0
    left = 0
    for right in range(len(events)):
        while events[right] - events[left] >= window_sec:
            left += 1
        max_count = max(max_count, right - left + 1)
    return max_count


def max_tpm_sliding_window(events: list[tuple[float, int]], window_sec: int = 60) -> int:
    if not events:
        return 0
    events = sorted(events, key=lambda x: x[0])
    max_tokens = 0
    left = 0
    running = 0
    for right in range(len(events)):
        running += events[right][1]
        while events[right][0] - events[left][0] >= window_sec:
            running -= events[left][1]
            left += 1
        max_tokens = max(max_tokens, running)
    return max_tokens


def rpm_per_minute(completions: list[tuple[float, str, int]], model: str | None = None) -> dict[int, int]:
    buckets: dict[int, int] = defaultdict(int)
    for ts, m, _ in completions:
        if model and m != model:
            continue
        minute = int(ts // 60)
        buckets[minute] += 1
    return dict(buckets)


def avg_rpm_in_window(timestamps: list[float], window_start: float, window_end: float) -> float:
    if window_end <= window_start:
        return 0.0
    count = sum(1 for ts in timestamps if window_start <= ts <= window_end)
    minutes = (window_end - window_start) / 60.0
    return count / minutes if minutes > 0 else 0.0


def wait_for_completions(prefix: str, expected: int, timeout_sec: int = 600) -> dict[str, int]:
    end = time.time() + timeout_sec
    log(f"waiting for completions prefix={prefix} expected={expected:,} timeout={timeout_sec}s")
    while time.time() < end:
        rows = psql(
            f"SELECT state, count(*) FROM requests WHERE request_id LIKE '{prefix}%' GROUP BY state"
        )
        states = {}
        for line in rows.splitlines():
            if "|" in line:
                s, c = line.split("|")
                states[s] = int(c)
        terminal = states.get("SUCCEEDED", 0) + states.get("FAILED", 0) + states.get("EXPIRED", 0)
        queued = states.get("QUEUED", 0) + states.get("IN_FLIGHT", 0)
        log(
            f"drain status terminal={terminal:,}/{expected:,} "
            f"queued={states.get('QUEUED', 0):,} in_flight={states.get('IN_FLIGHT', 0):,}"
        )
        if terminal >= expected:
            log(f"drain complete succeeded={states.get('SUCCEEDED', 0):,} failed={states.get('FAILED', 0):,}")
            return states
        time.sleep(5)
    rows = psql(
        f"SELECT state, count(*) FROM requests WHERE request_id LIKE '{prefix}%' GROUP BY state"
    )
    states = {}
    for line in rows.splitlines():
        if "|" in line:
            s, c = line.split("|")
            states[s] = int(c)
    return states


def percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    sorted_values = sorted(values)
    rank = (len(sorted_values) - 1) * (p / 100.0)
    low = int(rank)
    high = min(low + 1, len(sorted_values) - 1)
    weight = rank - low
    return sorted_values[low] + (sorted_values[high] - sorted_values[low]) * weight


def latency_percentiles_ms(since_epoch: float | None = None, batch_id: str | None = None, prefix: str | None = None) -> dict[str, float]:
    clauses = ["completed_at IS NOT NULL"]
    if since_epoch is not None:
        since = datetime.fromtimestamp(since_epoch, tz=timezone.utc).isoformat()
        clauses.append(f"submitted_at >= '{since}'")
    if batch_id is not None:
        clauses.append(f"batch_id = '{batch_id}'")
    if prefix is not None:
        clauses.append(f"request_id LIKE '{prefix}%'")
    where = " AND ".join(clauses)
    rows = psql(
        f"SELECT extract(epoch from (completed_at - submitted_at)) * 1000 "
        f"FROM requests WHERE {where}"
    )
    latencies = [float(line) for line in rows.splitlines() if line.strip()]
    return {
        "p50_ms": round(percentile(latencies, 50), 1),
        "p95_ms": round(percentile(latencies, 95), 1),
        "p99_ms": round(percentile(latencies, 99), 1),
        "samples": len(latencies),
    }


def request_counts(since_epoch: float | None = None, batch_id: str | None = None, prefix: str | None = None) -> dict[str, int]:
    clauses = ["TRUE"]
    if since_epoch is not None:
        since = datetime.fromtimestamp(since_epoch, tz=timezone.utc).isoformat()
        clauses.append(f"submitted_at >= '{since}'")
    if batch_id is not None:
        clauses.append(f"batch_id = '{batch_id}'")
    if prefix is not None:
        clauses.append(f"request_id LIKE '{prefix}%'")
    where = " AND ".join(clauses)
    rows = psql(f"SELECT state, count(*) FROM requests WHERE {where} GROUP BY state")
    states: dict[str, int] = {}
    for line in rows.splitlines():
        if "|" in line:
            state, count = line.split("|")
            states[state] = int(count)
    submitted = sum(states.values())
    succeeded = states.get("SUCCEEDED", 0)
    failed = states.get("FAILED", 0)
    expired = states.get("EXPIRED", 0)
    completed = succeeded + failed + expired
    return {
        "submitted": submitted,
        "completed": completed,
        "successful": succeeded,
        "failed": failed,
        "expired": expired,
        "queued": states.get("QUEUED", 0),
        "in_flight": states.get("IN_FLIGHT", 0),
        "accounted_rate_pct": round((completed / submitted * 100.0) if submitted else 0.0, 2),
        "success_rate_pct": round((succeeded / submitted * 100.0) if submitted else 0.0, 2),
    }


def observed_limits_per_model(
    completions: list[tuple[float, str, int]],
    configured: dict[str, dict[str, int]],
) -> dict[str, dict[str, Any]]:
    observed: dict[str, dict[str, Any]] = {}
    for model, limits in configured.items():
        events = [(ts, tok) for ts, m, tok in completions if m == model]
        ts_list = [ts for ts, _ in events]
        observed[model] = {
            "configured_rpm": limits["rpm"],
            "configured_tpm": limits["tpm"],
            "observed_max_rpm_60s": max_in_sliding_window(ts_list, 60),
            "observed_max_tpm_60s": max_tpm_sliding_window(events, 60),
            "completed_requests": len(events),
        }
    return observed


def write_benchmark_report(results: list[ScenarioResult]) -> None:
    report_path = REPO_ROOT / "BENCHMARK.md"
    with open(report_path, "w") as f:
        f.write("# Benchmark & Validation Report\n\n")
        f.write(f"Generated: {datetime.now(timezone.utc).isoformat()}\n\n")
        f.write("## Environment\n\n")
        f.write("- Docker Compose stack on local machine\n")
        f.write("- Java 21 microservices + Kafka + Postgres + Redis\n\n")
        f.write("## Scenario Results\n\n")
        for r in results:
            f.write(f"### {r.name}: {'PASS' if r.passed else 'FAIL'}\n\n")
            summary = r.details.get("summary")
            if summary:
                f.write("#### Run summary\n\n")
                f.write("| Metric | Value |\n")
                f.write("|--------|-------|\n")
                for key, value in summary.items():
                    f.write(f"| {key} | {value} |\n")
                f.write("\n")
            latency = r.details.get("latency_ms")
            if latency:
                f.write("#### Latency (submit → complete)\n\n")
                f.write("| Percentile | ms |\n")
                f.write("|------------|-----|\n")
                f.write(f"| p50 | {latency['p50_ms']} |\n")
                f.write(f"| p95 | {latency['p95_ms']} |\n")
                f.write(f"| p99 | {latency['p99_ms']} |\n")
                f.write(f"| samples | {latency['samples']} |\n\n")
            limits = r.details.get("limits_per_model")
            if limits:
                f.write("#### Configured vs observed limits (per model)\n\n")
                f.write("| Model | Config RPM | Observed max RPM (60s) | Config TPM | Observed max TPM (60s) | Completed |\n")
                f.write("|-------|------------|------------------------|------------|-------------------------|----------|\n")
                for model, stats in limits.items():
                    f.write(
                        f"| {model} | {stats['configured_rpm']:,} | {stats['observed_max_rpm_60s']:,} "
                        f"| {stats['configured_tpm']:,} | {stats['observed_max_tpm_60s']:,} "
                        f"| {stats['completed_requests']:,} |\n"
                    )
                f.write("\n")
            timing = r.details.get("timing")
            if timing:
                f.write("#### Batch / callback timing\n\n")
                f.write("| Milestone | ms |\n")
                f.write("|-----------|-----|\n")
                for key, value in timing.items():
                    f.write(f"| {key} | {value} |\n")
                f.write("\n")
            f.write("<details>\n<summary>Full JSON details</summary>\n\n")
            f.write("```json\n")
            f.write(json.dumps(r.details, indent=2))
            f.write("\n```\n\n</details>\n\n")


def scenario1() -> ScenarioResult:
    log_step("SCENARIO 1: Reach provider capacity (50K RPM / 100M TPM)")

    set_model_limits("model-a", 50_000, 100_000_000)
    set_model_limits("model-b", 20_000, 40_000_000)

    start = time.time()
    duration_sec = 300  # 5 minutes
    target_rate = 900

    submitted = submit_load_duration("s1", "model-a", target_rate, duration_sec)

    states = wait_for_completions("s1-", submitted, timeout_sec=600)
    succeeded_completions = fetch_completions(start, states=("SUCCEEDED",), prefix="s1-")
    all_completions = fetch_completions(start, prefix="s1-")

    model_a_succeeded = [(ts, tok) for ts, m, tok in succeeded_completions if m == "model-a"]
    rpm_timestamps = [ts for ts, _ in model_a_succeeded]
    tpm_events = model_a_succeeded

    max_rpm = max_in_sliding_window(rpm_timestamps, 60)
    max_tpm = max_tpm_sliding_window(tpm_events, 60)

    warmup_end = start + 60
    load_end = start + duration_sec
    avg_rpm = avg_rpm_in_window(rpm_timestamps, warmup_end, load_end)

    total_terminal = states.get("SUCCEEDED", 0) + states.get("FAILED", 0) + states.get("EXPIRED", 0)
    queued = states.get("QUEUED", 0) + states.get("IN_FLIGHT", 0)
    accounted = total_terminal + queued

    passed = (
        max_rpm <= 50_000
        and max_tpm <= 100_000_000
        and avg_rpm >= 45_000
        and accounted >= submitted * 0.99
    )

    details = {
        "summary": {
            "submitted": submitted,
            "completed": total_terminal,
            "successful": states.get("SUCCEEDED", 0),
            "failed": states.get("FAILED", 0),
            "expired": states.get("EXPIRED", 0),
            "accounted_rate_pct": round((accounted / submitted * 100.0) if submitted else 0.0, 2),
            "success_rate_pct": round(
                (states.get("SUCCEEDED", 0) / submitted * 100.0) if submitted else 0.0, 2
            ),
        },
        "latency_ms": latency_percentiles_ms(since_epoch=start, prefix="s1-"),
        "limits_per_model": observed_limits_per_model(
            succeeded_completions,
            {"model-a": {"rpm": 50_000, "tpm": 100_000_000}},
        ),
        "submitted": submitted,
        "states": states,
        "max_rpm_60s_window": max_rpm,
        "max_tpm_60s_window": max_tpm,
        "avg_rpm_post_warmup": round(avg_rpm, 1),
        "rpm_limit": 50_000,
        "tpm_limit": 100_000_000,
        "duration_sec": duration_sec,
    }
    latency = details["latency_ms"]
    log_metrics(
        f"SCENARIO 1 {'PASS' if passed else 'FAIL'}",
        {
            "submitted": submitted,
            "completed": total_terminal,
            "accounted_rate_pct": details["summary"]["accounted_rate_pct"],
            "success_rate_pct": details["summary"]["success_rate_pct"],
            "max_rpm_60s": max_rpm,
            "max_tpm_60s": max_tpm,
            "avg_rpm_post_warmup": round(avg_rpm, 1),
            "latency_p50_ms": latency["p50_ms"],
            "latency_p95_ms": latency["p95_ms"],
            "latency_p99_ms": latency["p99_ms"],
        },
    )
    return ScenarioResult("Scenario 1: Capacity", passed, details)


def scenario2() -> ScenarioResult:
    log_step("SCENARIO 2: Different models + changing limits")

    set_model_limits("model-a", 30_000, 60_000_000)
    set_model_limits("model-b", 20_000, 40_000_000)

    start = time.time()
    prefix = "s2"

    # Phase 1: 0-90s full load both models
    log("phase 1 (0-90s): full load on model-a and model-b")
    pool_a = ThreadPoolExecutor(max_workers=1)
    pool_b = ThreadPoolExecutor(max_workers=1)
    fa = pool_a.submit(submit_load_duration, f"{prefix}-a", "model-a", 800, 90)
    fb = pool_b.submit(submit_load_duration, f"{prefix}-b", "model-b", 500, 90)

    time.sleep(45)
    log("phase 2 (45s): throttle model-a to 5K RPM")
    set_model_limits("model-a", 5_000, 60_000_000)

    time.sleep(45)
    log("phase 3 (90s): restore model-a to 30K RPM")
    set_model_limits("model-a", 30_000, 60_000_000)

    submitted_a = fa.result()
    submitted_b = fb.result()
    pool_a.shutdown()
    pool_b.shutdown()

    log(f"submitted model-a={submitted_a:,} model-b={submitted_b:,}")
    log("waiting 120s for queue drain before measuring...")
    time.sleep(120)

    completions = fetch_completions(start, states=("SUCCEEDED", "FAILED", "EXPIRED"), prefix=f"{prefix}-")
    succeeded_completions = fetch_completions(start, states=("SUCCEEDED",), prefix=f"{prefix}-")

    def analyze_model(model: str, rpm_limit: int, tpm_limit: int) -> dict:
        events = [(ts, tok) for ts, m, tok in succeeded_completions if m == model]
        ts_list = [ts for ts, _ in events]
        return {
            "max_rpm_60s": max_in_sliding_window(ts_list, 60),
            "max_tpm_60s": max_tpm_sliding_window(events, 60),
            "rpm_limit": rpm_limit,
            "tpm_limit": tpm_limit,
            "total_completed": len(events),
            "rpm_per_minute": rpm_per_minute([(ts, model, 1000) for ts in events], model),
        }

    a_stats = analyze_model("model-a", 30_000, 60_000_000)
    b_stats = analyze_model("model-b", 20_000, 40_000_000)

    # During throttle phase (45-90s from start), model-a should be <= 5K per minute
    throttle_start = start + 45
    throttle_end = start + 90
    throttle_events = [
        ts for ts, m, _ in succeeded_completions
        if m == "model-a" and throttle_start <= ts <= throttle_end
    ]
    throttle_max_rpm = max_in_sliding_window(throttle_events, 60) if throttle_events else 0

    # Model B should still process during A throttle
    b_during_throttle = [
        ts for ts, m, _ in succeeded_completions
        if m == "model-b" and throttle_start <= ts <= throttle_end
    ]

    passed = (
        a_stats["max_rpm_60s"] <= 30_000
        and b_stats["max_rpm_60s"] <= 20_000
        and throttle_max_rpm <= 5_500  # small tolerance
        and len(b_during_throttle) > 0
    )

    details = {
        "summary": {
            "submitted": submitted_a + submitted_b,
            "completed": a_stats["total_completed"] + b_stats["total_completed"],
            "successful": a_stats["total_completed"] + b_stats["total_completed"],
            "failed": 0,
            "expired": 0,
            "accounted_rate_pct": round(
                ((a_stats["total_completed"] + b_stats["total_completed"]) / (submitted_a + submitted_b) * 100.0)
                if (submitted_a + submitted_b) else 0.0,
                2,
            ),
            "success_rate_pct": round(
                ((a_stats["total_completed"] + b_stats["total_completed"]) / (submitted_a + submitted_b) * 100.0)
                if (submitted_a + submitted_b) else 0.0,
                2,
            ),
        },
        "latency_ms": latency_percentiles_ms(since_epoch=start, prefix=f"{prefix}-"),
        "limits_per_model": observed_limits_per_model(
            succeeded_completions,
            {
                "model-a": {"rpm": 30_000, "tpm": 60_000_000},
                "model-b": {"rpm": 20_000, "tpm": 40_000_000},
            },
        ),
        "submitted_a": submitted_a,
        "submitted_b": submitted_b,
        "model_a": a_stats,
        "model_b": b_stats,
        "throttle_phase_max_rpm_a": throttle_max_rpm,
        "model_b_completions_during_a_throttle": len(b_during_throttle),
    }
    log_metrics(
        f"SCENARIO 2 {'PASS' if passed else 'FAIL'}",
        {
            "submitted": submitted_a + submitted_b,
            "model_a_max_rpm": a_stats["max_rpm_60s"],
            "model_b_max_rpm": b_stats["max_rpm_60s"],
            "throttle_phase_max_rpm_a": throttle_max_rpm,
            "model_b_completions_during_a_throttle": len(b_during_throttle),
        },
    )
    return ScenarioResult("Scenario 2: Limit changes", passed, details)


def scenario3() -> ScenarioResult:
    log_step("SCENARIO 3: Async batch (10K) + callback retries")

    reset_callback_mock()
    set_model_limits("model-a", 50_000, 100_000_000)
    set_model_limits("model-b", 50_000, 100_000_000)

    batch_size = 10_000
    run_id = int(time.time())
    requests = []
    for i in range(batch_size):
        model = "model-a" if i % 2 == 0 else "model-b"
        requests.append({
            "requestId": f"s3-{run_id}-req-{i}",
            "model": model,
            "estimatedTokens": 100,
            "payload": {"i": i},
        })

    log(f"submitting batch size={batch_size:,} callback={CALLBACK_URL}")
    run_start = time.time()
    t0 = time.time()
    body = {"callbackUrl": CALLBACK_URL, "requests": requests}
    data = json.dumps(body).encode()
    req = urllib.request.Request(
        f"{BASE_URL}/v1/batches",
        data=data,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=120) as resp:
        ack = json.load(resp)
    ack_ms = (time.time() - t0) * 1000

    batch_id = ack["batchId"]
    log(f"batch ack in {ack_ms:.0f}ms batchId={batch_id}")

    log("waiting for batch COMPLETED...")
    end = time.time() + 1800
    batch_status = None
    batch_completed_at: float | None = None
    while time.time() < end:
        batch_status = api("GET", f"/v1/batches/{batch_id}")
        log(
            f"batch status={batch_status['status']} "
            f"succeeded={batch_status.get('succeeded', 0):,}/{batch_status.get('total', 0):,}"
        )
        if batch_status["status"] == "COMPLETED":
            batch_completed_at = time.time()
            break
        time.sleep(5)

    log("waiting for callback DELIVERED...")
    callback_delivered_at: float | None = None
    while time.time() < end:
        batch_status = api("GET", f"/v1/batches/{batch_id}")
        log(
            f"callback status={batch_status.get('callbackStatus')} "
            f"attempts={batch_status.get('callbackAttempts', 0)}"
        )
        if batch_status.get("callbackStatus") == "DELIVERED":
            callback_delivered_at = time.time()
            break
        time.sleep(3)

    # Fetch callback payload
    with urllib.request.urlopen("http://localhost:9000/received", timeout=5) as resp:
        callback_data = json.load(resp)

    # Verify request ids unique
    ids_db = psql(
        f"SELECT count(*), count(distinct request_id) FROM requests WHERE batch_id='{batch_id}'"
    )
    total_db, distinct_db = ids_db.split("|")

    last_callback = callback_data["received"][-1]["body"] if callback_data["received"] else {}
    counts = request_counts(batch_id=batch_id)
    completions = fetch_completions(run_start, states=("SUCCEEDED",), prefix=f"s3-{run_id}-req-")
    configured_limits = {
        "model-a": {"rpm": 50_000, "tpm": 100_000_000},
        "model-b": {"rpm": 50_000, "tpm": 100_000_000},
    }
    batch_processing_ms = round((batch_completed_at - run_start) * 1000, 1) if batch_completed_at else None
    callback_after_batch_ms = (
        round((callback_delivered_at - batch_completed_at) * 1000, 1)
        if batch_completed_at and callback_delivered_at else None
    )
    callback_total_ms = round((callback_delivered_at - run_start) * 1000, 1) if callback_delivered_at else None

    passed = (
        ack_ms < 1000
        and batch_status["status"] == "COMPLETED"
        and batch_status.get("callbackStatus") == "DELIVERED"
        and int(total_db) == batch_size
        and int(distinct_db) == batch_size
        and batch_status["total"] == last_callback.get("total")
        and batch_status["succeeded"] + batch_status["failed"] + batch_status["expired"] == batch_status["total"]
        and callback_data["count"] >= 3  # 2 rejects + 1 success
    )

    details = {
        "summary": {
            "submitted": counts["submitted"],
            "completed": counts["completed"],
            "successful": counts["successful"],
            "failed": counts["failed"],
            "expired": counts["expired"],
            "accounted_rate_pct": counts["accounted_rate_pct"],
            "success_rate_pct": counts["success_rate_pct"],
        },
        "latency_ms": latency_percentiles_ms(batch_id=batch_id),
        "limits_per_model": observed_limits_per_model(completions, configured_limits),
        "timing": {
            "batch_ack_ms": round(ack_ms, 1),
            "batch_processing_ms": batch_processing_ms,
            "callback_after_batch_ms": callback_after_batch_ms,
            "callback_total_ms": callback_total_ms,
            "callback_attempts": callback_data["count"],
        },
        "ack_ms": round(ack_ms, 1),
        "batch_status": batch_status,
        "callback_attempts": callback_data["count"],
        "db_total": int(total_db),
        "db_distinct_ids": int(distinct_db),
        "callback_summary": last_callback,
    }
    log_metrics(
        f"SCENARIO 3 {'PASS' if passed else 'FAIL'}",
        {
            "ack_ms": round(ack_ms, 1),
            "batch_processing_ms": batch_processing_ms,
            "callback_after_batch_ms": callback_after_batch_ms,
            "callback_attempts": callback_data["count"],
            "success_rate_pct": counts["success_rate_pct"],
            "latency_p50_ms": details["latency_ms"]["p50_ms"],
            "latency_p95_ms": details["latency_ms"]["p95_ms"],
        },
    )
    return ScenarioResult("Scenario 3: Batch + callback", passed, details)


def wait_for_service() -> None:
    for attempt in range(120):
        try:
            api("GET", "/actuator/health")
            return
        except Exception:
            if attempt % 10 == 0:
                log(f"waiting for inference-gateway at {BASE_URL} ({attempt * 2}s)")
            time.sleep(2)
    raise RuntimeError("inference-gateway not ready")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run assignment validation scenarios")
    parser.add_argument(
        "scenarios",
        nargs="*",
        choices=["1", "2", "3"],
        help="Scenarios to run (default: all). Example: validate.py 3",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    to_run = args.scenarios if args.scenarios else ["1", "2", "3"]
    log_step(f"validation run — scenarios: {', '.join(to_run)}")
    log(f"gateway={BASE_URL} callback={CALLBACK_URL}")

    wait_for_service()
    reset_pipeline_state()
    results: list[ScenarioResult] = []

    if "1" in to_run:
        results.append(scenario1())
    if "2" in to_run:
        results.append(scenario2())
    if "3" in to_run:
        results.append(scenario3())

    log_step("SUMMARY")
    all_pass = True
    for r in results:
        status = "PASS" if r.passed else "FAIL"
        log(f"[{status}] {r.name}")
        if not r.passed:
            all_pass = False

    write_benchmark_report(results)

    log(f"report written to {REPO_ROOT / 'BENCHMARK.md'}")
    return 0 if all_pass else 1


if __name__ == "__main__":
    sys.exit(main())
