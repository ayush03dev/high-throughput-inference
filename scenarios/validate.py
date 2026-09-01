#!/usr/bin/env python3
"""Run assignment validation scenarios against a live stack."""

from __future__ import annotations

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
    api("PUT", f"/v1/admin/models/{name}", {"rpmLimit": rpm, "tpmLimit": tpm})


def reset_callback_mock() -> None:
    req = urllib.request.Request("http://localhost:9000/reset", method="POST")
    with urllib.request.urlopen(req, timeout=5):
        pass


def reset_pipeline_state() -> None:
    """Clear queued work so scenarios start from a clean slate."""
    psql("TRUNCATE requests, batches")
    reset_callback_mock()
    print("Pipeline state reset (truncated requests + batches)")


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
    end = time.time() + duration_sec
    interval = 1.0 / rate
    i = 0
    pool = ThreadPoolExecutor(max_workers=200)

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

    pool.shutdown(wait=False)
    return submitted


def fetch_completions(since_epoch: float) -> list[tuple[float, str, int]]:
    since = datetime.fromtimestamp(since_epoch, tz=timezone.utc).isoformat()
    rows = psql(
        f"SELECT extract(epoch from completed_at)::bigint, model, estimated_tokens "
        f"FROM requests WHERE completed_at IS NOT NULL AND submitted_at >= '{since}' "
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


def wait_for_completions(prefix: str, expected: int, timeout_sec: int = 600) -> dict[str, int]:
    end = time.time() + timeout_sec
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
        if terminal >= expected:
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
        "completion_rate_pct": round((completed / submitted * 100.0) if submitted else 0.0, 2),
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

    print("\n" + "=" * 60)
    print("SCENARIO 1: Reach provider capacity (50K RPM / 100M TPM)")
    print("=" * 60)

    set_model_limits("model-a", 50_000, 100_000_000)
    set_model_limits("model-b", 20_000, 40_000_000)

    start = time.time()
    duration_sec = 300  # 5 minutes
    # Saturate the 50K RPM limiter (~833 req/s); local stack may still fall short on completion throughput.
    target_rate = 900

    print(f"Submitting ~{target_rate}/s to model-a for {duration_sec}s...")
    submitted = submit_load_duration("s1", "model-a", target_rate, duration_sec)
    print(f"Submitted: {submitted}")

    print("Waiting for queue to drain (up to 10 min)...")
    states = wait_for_completions("s1-", submitted, timeout_sec=600)
    completions = fetch_completions(start)

    model_a = [(ts, tok) for ts, m, tok in completions if m == "model-a"]
    rpm_timestamps = [ts for ts, _ in model_a]
    tpm_events = model_a

    max_rpm = max_in_sliding_window(rpm_timestamps, 60)
    max_tpm = max_tpm_sliding_window(tpm_events, 60)

    # Per-minute buckets after warm-up (skip first 60s)
    warmup_end = start + 60
    post_warmup = [ts for ts in rpm_timestamps if ts >= warmup_end]
    buckets = rpm_per_minute([(ts, "model-a", 1000) for ts in post_warmup])
    avg_rpm = (sum(buckets.values()) / max(len(buckets), 1)) if buckets else 0

    total_terminal = states.get("SUCCEEDED", 0) + states.get("FAILED", 0) + states.get("EXPIRED", 0)
    queued = states.get("QUEUED", 0) + states.get("IN_FLIGHT", 0)
    accounted = total_terminal + queued

    passed = (
        max_rpm <= 50_000
        and max_tpm <= 100_000_000
        and avg_rpm >= 45_000  # 90% of 50K RPM target
        and accounted >= submitted * 0.99
    )

    details = {
        "summary": {
            "submitted": submitted,
            "completed": total_terminal,
            "successful": states.get("SUCCEEDED", 0),
            "failed": states.get("FAILED", 0),
            "expired": states.get("EXPIRED", 0),
            "completion_rate_pct": round((total_terminal / submitted * 100.0) if submitted else 0.0, 2),
        },
        "latency_ms": latency_percentiles_ms(since_epoch=start),
        "limits_per_model": observed_limits_per_model(
            completions,
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
    print(json.dumps(details, indent=2))
    return ScenarioResult("Scenario 1: Capacity", passed, details)


def scenario2() -> ScenarioResult:
    print("\n" + "=" * 60)
    print("SCENARIO 2: Different models + changing limits")
    print("=" * 60)

    set_model_limits("model-a", 30_000, 60_000_000)
    set_model_limits("model-b", 20_000, 40_000_000)

    start = time.time()
    prefix = "s2"

    # Phase 1: 0-90s full load both models
    print("Phase 1 (0-90s): both models at full limits, high load...")
    pool_a = ThreadPoolExecutor(max_workers=1)
    pool_b = ThreadPoolExecutor(max_workers=1)
    fa = pool_a.submit(submit_load_duration, f"{prefix}-a", "model-a", 800, 90)
    fb = pool_b.submit(submit_load_duration, f"{prefix}-b", "model-b", 500, 90)

    time.sleep(45)
    print("Phase 2 (45s): throttle model-a to 5K RPM...")
    set_model_limits("model-a", 5_000, 60_000_000)

    time.sleep(45)
    print("Phase 3 (90s): restore model-a to 30K RPM...")
    set_model_limits("model-a", 30_000, 60_000_000)

    submitted_a = fa.result()
    submitted_b = fb.result()
    pool_a.shutdown()
    pool_b.shutdown()

    print(f"Submitted model-a: {submitted_a}, model-b: {submitted_b}")
    print("Waiting for drain...")
    time.sleep(120)

    completions = fetch_completions(start)

    def analyze_model(model: str, rpm_limit: int, tpm_limit: int) -> dict:
        events = [(ts, tok) for ts, m, tok in completions if m == model]
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
    throttle_events = [ts for ts, m, _ in completions if m == "model-a" and throttle_start <= ts <= throttle_end]
    throttle_max_rpm = max_in_sliding_window(throttle_events, 60) if throttle_events else 0

    # Model B should still process during A throttle
    b_during_throttle = [ts for ts, m, _ in completions if m == "model-b" and throttle_start <= ts <= throttle_end]

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
            "completion_rate_pct": round(
                ((a_stats["total_completed"] + b_stats["total_completed"]) / (submitted_a + submitted_b) * 100.0)
                if (submitted_a + submitted_b) else 0.0,
                2,
            ),
        },
        "latency_ms": latency_percentiles_ms(since_epoch=start, prefix=f"{prefix}-"),
        "limits_per_model": observed_limits_per_model(
            completions,
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
    print(json.dumps(details, indent=2))
    return ScenarioResult("Scenario 2: Limit changes", passed, details)


def scenario3() -> ScenarioResult:
    print("\n" + "=" * 60)
    print("SCENARIO 3: Async batch (10K) + callback retries")
    print("=" * 60)

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

    print(f"Submitting batch of {batch_size}...")
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
    print(f"Ack in {ack_ms:.0f}ms: {batch_id}")

    print("Waiting for batch completion...")
    end = time.time() + 1800
    batch_status = None
    batch_completed_at: float | None = None
    while time.time() < end:
        batch_status = api("GET", f"/v1/batches/{batch_id}")
        if batch_status["status"] == "COMPLETED":
            batch_completed_at = time.time()
            break
        time.sleep(5)

    print("Waiting for callback delivery...")
    callback_delivered_at: float | None = None
    while time.time() < end:
        batch_status = api("GET", f"/v1/batches/{batch_id}")
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
    completions = fetch_completions(run_start)
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
            "completion_rate_pct": counts["completion_rate_pct"],
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
    print(json.dumps(details, indent=2))
    return ScenarioResult("Scenario 3: Batch + callback", passed, details)


def wait_for_service() -> None:
    for attempt in range(120):
        try:
            api("GET", "/actuator/health")
            return
        except Exception:
            if attempt % 10 == 0:
                print(f"Waiting for inference-gateway... ({attempt * 2}s)")
            time.sleep(2)
    raise RuntimeError("inference-gateway not ready")


def main() -> int:
    wait_for_service()
    reset_pipeline_state()
    results: list[ScenarioResult] = []

    selected = set(sys.argv[1:]) if len(sys.argv) > 1 else None

    if selected is None or "1" in selected:
        results.append(scenario1())
    if selected is None or "2" in selected:
        results.append(scenario2())
    if selected is None or "3" in selected:
        results.append(scenario3())

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    all_pass = True
    for r in results:
        status = "PASS" if r.passed else "FAIL"
        print(f"  [{status}] {r.name}")
        if not r.passed:
            all_pass = False

    write_benchmark_report(results)

    print(f"\nReport written to {REPO_ROOT / 'BENCHMARK.md'}")
    return 0 if all_pass else 1


if __name__ == "__main__":
    sys.exit(main())
