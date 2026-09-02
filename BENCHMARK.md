# Benchmark & Validation Report

Each validation run appends a new dated section below — this file is a running history, not just the latest run.

## Run — 2026-09-02T15:44:13.918740+00:00

### Environment

- Docker Compose stack on local machine
- Java 21 microservices + Kafka + Postgres + Redis

### Scenario Results

#### Scenario 1: Capacity: PASS

##### Run summary

| Metric | Value |
|--------|-------|
| submitted | 262080 |
| completed | 262080 |
| successful | 262080 |
| failed | 0 |
| expired | 0 |
| accounted_rate_pct | 100.0 |
| success_rate_pct | 100.0 |

##### Latency (submit → complete)

| Percentile | ms |
|------------|-----|
| p50 | 24.7 |
| p95 | 27865.0 |
| p99 | 48747.7 |
| samples | 262080 |

##### Configured vs observed limits (per model)

| Model | Config RPM | Observed max RPM (60s) | Config TPM | Observed max TPM (60s) | Completed |
|-------|------------|------------------------|------------|-------------------------|----------|
| model-a | 50,000 | 50,018 | 100,000,000 | 50,018,000 | 262,080 |

<details>
<summary>Full JSON details</summary>

```json
{
  "summary": {
    "submitted": 262080,
    "completed": 262080,
    "successful": 262080,
    "failed": 0,
    "expired": 0,
    "accounted_rate_pct": 100.0,
    "success_rate_pct": 100.0
  },
  "latency_ms": {
    "p50_ms": 24.7,
    "p95_ms": 27865.0,
    "p99_ms": 48747.7,
    "samples": 262080
  },
  "limits_per_model": {
    "model-a": {
      "configured_rpm": 50000,
      "configured_tpm": 100000000,
      "observed_max_rpm_60s": 50018,
      "observed_max_tpm_60s": 50018000,
      "completed_requests": 262080
    }
  },
  "submitted": 262080,
  "states": {
    "SUCCEEDED": 262080
  },
  "max_rpm_60s_window": 50018,
  "max_tpm_60s_window": 50018000,
  "avg_rpm_post_warmup": 50000.0,
  "rpm_limit": 50000,
  "tpm_limit": 100000000,
  "duration_sec": 300
}
```

</details>

#### Scenario 2: Limit changes: PASS

##### Run summary

| Metric | Value |
|--------|-------|
| submitted | 114190 |
| completed | 114190 |
| successful | 114190 |
| failed | 0 |
| expired | 0 |
| accounted_rate_pct | 100.0 |
| success_rate_pct | 100.0 |

##### Latency (submit → complete)

| Percentile | ms |
|------------|-----|
| p50 | 5973.4 |
| p95 | 101827.4 |
| p99 | 127648.8 |
| samples | 114190 |

##### Configured vs observed limits (per model)

| Model | Config RPM | Observed max RPM (60s) | Config TPM | Observed max TPM (60s) | Completed |
|-------|------------|------------------------|------------|-------------------------|----------|
| model-a | 30,000 | 29,000 | 60,000,000 | 29,000,000 | 70,240 |
| model-b | 20,000 | 20,005 | 40,000,000 | 20,005,000 | 43,950 |

<details>
<summary>Full JSON details</summary>

```json
{
  "summary": {
    "submitted": 114190,
    "completed": 114190,
    "successful": 114190,
    "failed": 0,
    "expired": 0,
    "accounted_rate_pct": 100.0,
    "success_rate_pct": 100.0
  },
  "latency_ms": {
    "p50_ms": 5973.4,
    "p95_ms": 101827.4,
    "p99_ms": 127648.8,
    "samples": 114190
  },
  "limits_per_model": {
    "model-a": {
      "configured_rpm": 30000,
      "configured_tpm": 60000000,
      "observed_max_rpm_60s": 29000,
      "observed_max_tpm_60s": 29000000,
      "completed_requests": 70240
    },
    "model-b": {
      "configured_rpm": 20000,
      "configured_tpm": 40000000,
      "observed_max_rpm_60s": 20005,
      "observed_max_tpm_60s": 20005000,
      "completed_requests": 43950
    }
  },
  "submitted_a": 70240,
  "submitted_b": 43950,
  "model_a": {
    "max_rpm_60s": 29000,
    "max_tpm_60s": 29000000,
    "rpm_limit": 30000,
    "tpm_limit": 60000000,
    "total_completed": 70240,
    "rpm_per_minute": {
      "29806060": 19959,
      "29806061": 9041,
      "29806062": 24591,
      "29806063": 16649
    }
  },
  "model_b": {
    "max_rpm_60s": 20005,
    "max_tpm_60s": 20005000,
    "rpm_limit": 20000,
    "tpm_limit": 40000000,
    "total_completed": 43950,
    "rpm_per_minute": {
      "29806060": 13238,
      "29806061": 19980,
      "29806062": 7499,
      "29806063": 3233
    }
  },
  "throttle_phase_max_rpm_a": 30,
  "model_b_completions_during_a_throttle": 14608
}
```

</details>

#### Scenario 3: Batch + callback: PASS

##### Run summary

| Metric | Value |
|--------|-------|
| submitted | 10000 |
| completed | 10000 |
| successful | 9533 |
| failed | 467 |
| expired | 0 |
| accounted_rate_pct | 100.0 |
| success_rate_pct | 95.33 |

##### Latency (submit → complete)

| Percentile | ms |
|------------|-----|
| p50 | 3209.1 |
| p95 | 5864.0 |
| p99 | 6092.9 |
| samples | 10000 |

##### Configured vs observed limits (per model)

| Model | Config RPM | Observed max RPM (60s) | Config TPM | Observed max TPM (60s) | Completed |
|-------|------------|------------------------|------------|-------------------------|----------|
| model-a | 50,000 | 4,759 | 100,000,000 | 475,900 | 4,759 |
| model-b | 50,000 | 4,774 | 100,000,000 | 477,400 | 4,774 |

##### Batch / callback timing

| Milestone | ms |
|-----------|-----|
| batch_ack_ms | 106.4 |
| batch_processing_ms | 10126.9 |
| callback_after_batch_ms | 3.4 |
| callback_total_ms | 10130.2 |
| callback_attempts | 3 |

<details>
<summary>Full JSON details</summary>

```json
{
  "summary": {
    "submitted": 10000,
    "completed": 10000,
    "successful": 9533,
    "failed": 467,
    "expired": 0,
    "accounted_rate_pct": 100.0,
    "success_rate_pct": 95.33
  },
  "latency_ms": {
    "p50_ms": 3209.1,
    "p95_ms": 5864.0,
    "p99_ms": 6092.9,
    "samples": 10000
  },
  "limits_per_model": {
    "model-a": {
      "configured_rpm": 50000,
      "configured_tpm": 100000000,
      "observed_max_rpm_60s": 4759,
      "observed_max_tpm_60s": 475900,
      "completed_requests": 4759
    },
    "model-b": {
      "configured_rpm": 50000,
      "configured_tpm": 100000000,
      "observed_max_rpm_60s": 4774,
      "observed_max_tpm_60s": 477400,
      "completed_requests": 4774
    }
  },
  "timing": {
    "batch_ack_ms": 106.4,
    "batch_processing_ms": 10126.9,
    "callback_after_batch_ms": 3.4,
    "callback_total_ms": 10130.2,
    "callback_attempts": 3
  },
  "ack_ms": 106.4,
  "batch_status": {
    "batchId": "batch-47510b3b-1747-4b1b-ada2-4617dc2634bd",
    "status": "COMPLETED",
    "total": 10000,
    "succeeded": 9533,
    "failed": 467,
    "expired": 0,
    "callbackStatus": "DELIVERED",
    "callbackAttempts": 3,
    "createdAt": "2026-09-02T15:44:03.627575Z",
    "completedAt": "2026-09-02T15:44:10.055428Z"
  },
  "callback_attempts": 3,
  "db_total": 10000,
  "db_distinct_ids": 10000,
  "callback_summary": {
    "batchId": "batch-47510b3b-1747-4b1b-ada2-4617dc2634bd",
    "status": "completed",
    "total": 10000,
    "succeeded": 9533,
    "failed": 467,
    "expired": 0,
    "resultsUrl": "/v1/batches/batch-47510b3b-1747-4b1b-ada2-4617dc2634bd/results",
    "results": null
  },
  "callback_matches_batch_status": true,
  "simulated_failure_rate": 0.05
}
```

</details>