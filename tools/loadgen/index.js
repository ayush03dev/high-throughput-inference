#!/usr/bin/env node
import { Command } from 'commander';
import { request, Agent } from 'undici';

const program = new Command();
program
  .requiredOption('--url <url>', 'Ingest API base URL')
  .option('--rate <n>', 'Requests per second (individual requests, not batches)', '100')
  .option('--duration <seconds>', 'Duration in seconds', '10')
  .option('--models <spec>', 'Model weights e.g. model-a:1.0,model-b:0.5', 'model-a:1.0')
  .option('--tokens <n>', 'Estimated tokens per request', '1000')
  .option('--batch-size <n>', 'Batch size (1 = single requests)', '1')
  .option('--callback-url <url>', 'Callback URL for batch mode')
  .option('--concurrency <n>', 'Max in-flight HTTP calls (submission and polling)', '256')
  .option('--track-sample <n>', 'Max individual requests to poll for completion/latency in single-request mode', '3000')
  .option('--poll-timeout <seconds>', 'How long to keep polling for completion after submission ends', '120')
  .option('--no-track', 'Skip completion polling entirely (submission-only run)')
  .parse();

const opts = program.opts();
const baseUrl = opts.url.replace(/\/$/, '');
const rate = parseFloat(opts.rate);
const durationSec = parseInt(opts.duration, 10);
const tokens = parseInt(opts.tokens, 10);
const batchSize = parseInt(opts.batchSize, 10);
const concurrency = parseInt(opts.concurrency, 10);
const trackSampleTarget = parseInt(opts.trackSample, 10);
const pollTimeoutSec = parseInt(opts.pollTimeout, 10);
const trackEnabled = opts.track !== false;
const runId = Date.now().toString(36);

const modelWeights = Object.fromEntries(
  opts.models.split(',').map((part) => {
    const [model, weight] = part.split(':');
    return [model.trim(), parseFloat(weight)];
  })
);

const TERMINAL_STATES = new Set(['SUCCEEDED', 'FAILED', 'EXPIRED', 'REJECTED']);
const agent = new Agent({ connections: Math.max(concurrency, 64), pipelining: 1 });

// ---- shared state ----
const startMs = Date.now();
let submittedTotal = 0;
let ackedTotal = 0;
let ackFailedTotal = 0;
const ackLatencies = [];
const trackedRequests = []; // {id, model, submitStartMs}
const batches = []; // {batchId, submitStartMs, size}
const submitBucketsByModel = {}; // model -> Map(secondKey -> count)
const tokenBucketsByModel = {}; // model -> Map(secondKey -> tokenSum)
let lastProgressLog = startMs;

function log(msg) {
  const elapsed = ((Date.now() - startMs) / 1000).toFixed(0);
  console.log(`[loadgen +${elapsed}s] ${msg}`);
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function percentile(arr, p) {
  if (arr.length === 0) return 0;
  const sorted = [...arr].sort((a, b) => a - b);
  const idx = Math.ceil((p / 100) * sorted.length) - 1;
  return sorted[Math.max(0, idx)];
}

function avg(arr) {
  if (arr.length === 0) return 0;
  return arr.reduce((a, b) => a + b, 0) / arr.length;
}

// Bounded-concurrency gate: acquire() resolves once a slot is free.
class Semaphore {
  constructor(max) {
    this.max = max;
    this.current = 0;
    this.queue = [];
  }

  acquire() {
    if (this.current < this.max) {
      this.current += 1;
      return Promise.resolve();
    }
    return new Promise((resolve) => this.queue.push(resolve));
  }

  release() {
    this.current -= 1;
    const next = this.queue.shift();
    if (next) {
      this.current += 1;
      next();
    }
  }
}

function pickModel() {
  const entries = Object.entries(modelWeights);
  const total = entries.reduce((sum, [, w]) => sum + w, 0);
  let r = Math.random() * total;
  for (const [model, weight] of entries) {
    r -= weight;
    if (r <= 0) return model;
  }
  return entries[0][0];
}

function bucketKey(tsMs) {
  return Math.floor(tsMs / 1000);
}

function recordSubmission(model, tsMs, tokenCount) {
  const key = bucketKey(tsMs);
  const counts = (submitBucketsByModel[model] ??= new Map());
  counts.set(key, (counts.get(key) || 0) + 1);
  const toks = (tokenBucketsByModel[model] ??= new Map());
  toks.set(key, (toks.get(key) || 0) + tokenCount);
}

// Densifies a second->value bucket map and returns the max sum over any
// `windowSec`-second sliding window (mirrors scenarios/validate.py).
function maxSlidingWindowSum(bucketMap, windowSec = 60) {
  if (!bucketMap || bucketMap.size === 0) return 0;
  const keys = [...bucketMap.keys()];
  const start = Math.min(...keys);
  const end = Math.max(...keys);
  const size = end - start + 1;
  const arr = new Array(size).fill(0);
  for (const [k, v] of bucketMap) arr[k - start] = v;
  let curSum = 0;
  let max = 0;
  for (let i = 0; i < size; i += 1) {
    curSum += arr[i];
    if (i >= windowSec) curSum -= arr[i - windowSec];
    if (curSum > max) max = curSum;
  }
  return max;
}

function maybeLogProgress() {
  const now = Date.now();
  const elapsed = (now - startMs) / 1000;
  const rps = elapsed > 0 ? (submittedTotal / elapsed).toFixed(1) : '0';
  log(`progress submitted=${submittedTotal.toLocaleString()} acked=${ackedTotal.toLocaleString()} ackFailed=${ackFailedTotal.toLocaleString()} avgSubmitRps=${rps}`);
  lastProgressLog = now;
}

async function submitSingle(idx, track) {
  const model = pickModel();
  const id = `req-${runId}-${idx}`;
  const body = {
    requestId: id,
    model,
    estimatedTokens: tokens,
    payload: { prompt: `load-${id}` },
  };
  const submitStartMs = Date.now();
  submittedTotal += 1;
  recordSubmission(model, submitStartMs, tokens);

  let res;
  try {
    res = await request(`${baseUrl}/v1/inference`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(body),
      dispatcher: agent,
    });
    await res.body.text();
  } catch (err) {
    ackFailedTotal += 1;
    return;
  }
  ackLatencies.push(Date.now() - submitStartMs);
  if (res.statusCode === 202) {
    ackedTotal += 1;
    if (track) trackedRequests.push({ id, model, submitStartMs });
  } else {
    ackFailedTotal += 1;
  }
}

async function submitBatch(idx) {
  const requests = [];
  for (let i = 0; i < batchSize; i += 1) {
    const id = `batch-${runId}-${idx}-req-${i}`;
    const model = pickModel();
    requests.push({
      requestId: id,
      model,
      estimatedTokens: tokens,
      payload: { prompt: id },
    });
  }
  const submitStartMs = Date.now();
  for (const r of requests) recordSubmission(r.model, submitStartMs, tokens);
  submittedTotal += requests.length;

  let res;
  let body;
  try {
    res = await request(`${baseUrl}/v1/batches`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ callbackUrl: opts.callbackUrl, requests }),
      dispatcher: agent,
    });
    body = await res.body.json();
  } catch (err) {
    ackFailedTotal += requests.length;
    return;
  }
  ackLatencies.push(Date.now() - submitStartMs);
  if (res.statusCode === 202 && body?.batchId) {
    ackedTotal += requests.length;
    batches.push({ batchId: body.batchId, submitStartMs, size: requests.length });
    log(`batch submitted batchId=${body.batchId} size=${requests.length} ackMs=${Date.now() - submitStartMs}`);
  } else {
    ackFailedTotal += requests.length;
  }
}

// Rate-paced, concurrency-bounded dispatch. Fires HTTP calls without
// serializing on each response — the previous implementation awaited every
// request in the main loop, which capped throughput at roughly 1/latency
// regardless of the configured --rate.
async function runSubmissionPhase() {
  const sem = new Semaphore(concurrency);
  const endAt = startMs + durationSec * 1000;
  // rate is requests/sec; in batch mode we submit batchSize requests per
  // batch, so batches must be dispatched at rate/batchSize per second.
  const unitsPerSecond = batchSize > 1 ? rate / batchSize : rate;
  const totalPlannedUnits = Math.max(1, Math.round(unitsPerSecond * durationSec));
  const sampleEvery = batchSize > 1 || !trackEnabled
    ? Infinity
    : Math.max(1, Math.floor(totalPlannedUnits / trackSampleTarget));

  let counter = 0;
  let inFlightDone = 0;

  async function fireOne(idx) {
    try {
      if (batchSize > 1) {
        await submitBatch(idx);
      } else {
        await submitSingle(idx, trackEnabled && idx % sampleEvery === 0);
      }
    } finally {
      sem.release();
      inFlightDone += 1;
    }
  }

  while (Date.now() < endAt) {
    const elapsedSec = (Date.now() - startMs) / 1000;
    const targetCount = Math.floor(elapsedSec * unitsPerSecond);
    while (counter < targetCount) {
      const idx = counter;
      counter += 1;
      await sem.acquire();
      fireOne(idx); // intentionally not awaited: dispatch is decoupled from completion
    }
    if (Date.now() - lastProgressLog >= 5000) maybeLogProgress();
    await sleep(5);
  }

  const drainDeadline = Date.now() + 30_000;
  while (inFlightDone < counter && Date.now() < drainDeadline) {
    await sleep(20);
  }
  return counter;
}

async function pollTrackedRequests() {
  if (trackedRequests.length === 0) {
    return { sampleSize: 0, completed: 0, successful: 0, failed: 0, expired: 0, rejected: 0, timedOut: 0, e2eLatencies: [] };
  }
  const sem = new Semaphore(concurrency);
  const pending = new Map(trackedRequests.map((r) => [r.id, r]));
  const terminal = [];
  const deadline = Date.now() + pollTimeoutSec * 1000;

  while (pending.size > 0 && Date.now() < deadline) {
    const batch = [...pending.values()];
    await Promise.all(
      batch.map(async (r) => {
        await sem.acquire();
        try {
          const res = await request(`${baseUrl}/v1/requests/${r.id}`, { method: 'GET', dispatcher: agent });
          const body = await res.body.json();
          if (res.statusCode === 200 && TERMINAL_STATES.has(body.state)) {
            pending.delete(r.id);
            terminal.push({ ...r, state: body.state, submittedAt: body.submittedAt, completedAt: body.completedAt });
          }
        } catch (_err) {
          // transient poll failure; retried next round
        } finally {
          sem.release();
        }
      })
    );
    if (pending.size > 0) await sleep(500);
  }

  const successful = terminal.filter((t) => t.state === 'SUCCEEDED').length;
  const failed = terminal.filter((t) => t.state === 'FAILED').length;
  const expired = terminal.filter((t) => t.state === 'EXPIRED').length;
  const rejected = terminal.filter((t) => t.state === 'REJECTED').length;
  const e2eLatencies = terminal
    .filter((t) => t.submittedAt && t.completedAt)
    .map((t) => Date.parse(t.completedAt) - Date.parse(t.submittedAt))
    .filter((ms) => Number.isFinite(ms) && ms >= 0);

  return {
    sampleSize: trackedRequests.length,
    completed: terminal.length,
    successful,
    failed,
    expired,
    rejected,
    timedOut: pending.size,
    e2eLatencies,
  };
}

async function pollBatches() {
  if (batches.length === 0) return [];
  const sem = new Semaphore(concurrency);
  const pending = new Map(batches.map((b) => [b.batchId, b]));
  const results = [];
  const deadline = Date.now() + pollTimeoutSec * 1000;

  while (pending.size > 0 && Date.now() < deadline) {
    await Promise.all(
      [...pending.values()].map(async (b) => {
        await sem.acquire();
        try {
          const res = await request(`${baseUrl}/v1/batches/${b.batchId}`, { method: 'GET', dispatcher: agent });
          const body = await res.body.json();
          if (res.statusCode === 200 && body.status === 'COMPLETED') {
            pending.delete(b.batchId);
            results.push({
              batchId: b.batchId,
              size: b.size,
              timedOut: false,
              total: body.total,
              succeeded: body.succeeded,
              failed: body.failed,
              expired: body.expired,
              callbackStatus: body.callbackStatus,
              callbackAttempts: body.callbackAttempts,
              createdAt: body.createdAt,
              completedAt: body.completedAt,
              completionWallMs: Date.now() - b.submitStartMs,
            });
          }
        } catch (_err) {
          // transient poll failure; retried next round
        } finally {
          sem.release();
        }
      })
    );
    if (pending.size > 0) await sleep(1000);
  }

  for (const b of pending.values()) {
    results.push({ batchId: b.batchId, size: b.size, timedOut: true });
  }
  return results;
}

function perModelSubmittedSummary() {
  const out = {};
  for (const model of Object.keys(submitBucketsByModel)) {
    out[model] = {
      submitted: [...submitBucketsByModel[model].values()].reduce((a, b) => a + b, 0),
      observedMaxRpm60s: maxSlidingWindowSum(submitBucketsByModel[model], 60),
      observedMaxTpm60s: maxSlidingWindowSum(tokenBucketsByModel[model], 60),
    };
  }
  return out;
}

function buildReport(submissionElapsedSec, completionStats, batchResults) {
  const base = {
    config: {
      url: baseUrl,
      rateReqPerSec: rate,
      durationSec,
      models: opts.models,
      tokensPerRequest: tokens,
      batchSize,
      concurrency,
      trackingEnabled: trackEnabled,
    },
    submission: {
      submitted: submittedTotal,
      acked: ackedTotal,
      ackFailed: ackFailedTotal,
      elapsedSec: Math.round(submissionElapsedSec * 10) / 10,
      achievedSubmitRps: submissionElapsedSec > 0 ? Math.round(submittedTotal / submissionElapsedSec) : 0,
      ackLatencyMs: {
        p50: percentile(ackLatencies, 50),
        p95: percentile(ackLatencies, 95),
        p99: percentile(ackLatencies, 99),
      },
    },
    perModelSubmitted: perModelSubmittedSummary(),
  };

  if (!trackEnabled) return base;

  if (batchSize > 1) {
    const completedBatches = batchResults.filter((b) => !b.timedOut);
    return {
      ...base,
      batches: {
        submitted: batches.length,
        completed: completedBatches.length,
        timedOut: batchResults.length - completedBatches.length,
        totalRequests: completionStats.completed,
        successful: completionStats.successful,
        failed: completionStats.failed,
        expired: completionStats.expired,
        successRate: completionStats.completed > 0 ? +((100 * completionStats.successful) / completionStats.completed).toFixed(2) : 0,
        batchCompletionMs: {
          p50: percentile(completedBatches.map((b) => b.completionWallMs), 50),
          p95: percentile(completedBatches.map((b) => b.completionWallMs), 95),
          p99: percentile(completedBatches.map((b) => b.completionWallMs), 99),
        },
        e2eLatencyMs: {
          p50: percentile(completionStats.e2eLatencies, 50),
          p95: percentile(completionStats.e2eLatencies, 95),
          p99: percentile(completionStats.e2eLatencies, 99),
        },
        callback: {
          delivered: completedBatches.filter((b) => b.callbackStatus === 'DELIVERED').length,
          failed: completedBatches.filter((b) => b.callbackStatus === 'FAILED').length,
          pending: completedBatches.filter((b) => b.callbackStatus === 'PENDING').length,
          avgAttempts: +avg(completedBatches.map((b) => b.callbackAttempts || 0)).toFixed(2),
        },
        details: batchResults,
      },
    };
  }

  return {
    ...base,
    completion: {
      sampleSize: completionStats.sampleSize,
      sampleFraction: completionStats.sampleSize > 0 ? +(completionStats.sampleSize / submittedTotal).toFixed(4) : 0,
      completed: completionStats.completed,
      successful: completionStats.successful,
      failed: completionStats.failed,
      expired: completionStats.expired,
      rejected: completionStats.rejected,
      timedOut: completionStats.timedOut,
      completionRate: completionStats.sampleSize > 0 ? +((100 * completionStats.completed) / completionStats.sampleSize).toFixed(2) : 0,
      successRate: completionStats.completed > 0 ? +((100 * completionStats.successful) / completionStats.completed).toFixed(2) : 0,
      e2eLatencyMs: {
        p50: percentile(completionStats.e2eLatencies, 50),
        p95: percentile(completionStats.e2eLatencies, 95),
        p99: percentile(completionStats.e2eLatencies, 99),
      },
      note: 'completion/latency stats are measured on a sampled subset of submitted requests (see sampleSize/sampleFraction) to bound polling overhead at high request rates. For exact full-population verification of a scenario, use scenarios/validate.py, which reads the database directly.',
    },
  };
}

async function main() {
  if (batchSize > 1 && !opts.callbackUrl) {
    console.error('[loadgen] --batch-size > 1 requires --callback-url');
    process.exit(1);
  }

  log(`starting url=${baseUrl} rate=${rate} req/s duration=${durationSec}s models=${opts.models} tokens=${tokens} batchSize=${batchSize} concurrency=${concurrency}`);
  if (batchSize > 1) {
    log(`batch mode: batchSize=${batchSize} callback=${opts.callbackUrl} effectiveBatchRate=${(rate / batchSize).toFixed(2)}/s`);
  }
  log(trackEnabled
    ? `completion tracking enabled: sampleTarget=${trackSampleTarget} pollTimeout=${pollTimeoutSec}s`
    : 'completion tracking disabled (--no-track): submission-only run, report will not include completion/success/latency stats');

  await runSubmissionPhase();
  const submissionElapsedSec = (Date.now() - startMs) / 1000;
  log(`submission phase complete: submitted=${submittedTotal} acked=${ackedTotal} ackFailed=${ackFailedTotal} elapsed=${submissionElapsedSec.toFixed(1)}s achievedSubmitRps=${Math.round(submittedTotal / submissionElapsedSec)}`);

  let completionStats = { sampleSize: 0, completed: 0, successful: 0, failed: 0, expired: 0, rejected: 0, timedOut: 0, e2eLatencies: [] };
  let batchResults = [];

  if (trackEnabled) {
    if (batchSize > 1) {
      log(`polling ${batches.length} batches for completion (timeout=${pollTimeoutSec}s)...`);
      batchResults = await pollBatches();
      const completedBatches = batchResults.filter((b) => !b.timedOut);
      completionStats = {
        completed: completedBatches.reduce((s, b) => s + b.total, 0),
        successful: completedBatches.reduce((s, b) => s + b.succeeded, 0),
        failed: completedBatches.reduce((s, b) => s + b.failed, 0),
        expired: completedBatches.reduce((s, b) => s + (b.expired || 0), 0),
        e2eLatencies: completedBatches
          .filter((b) => b.createdAt && b.completedAt)
          .map((b) => Date.parse(b.completedAt) - Date.parse(b.createdAt))
          .filter((ms) => Number.isFinite(ms) && ms >= 0),
      };
    } else {
      log(`polling ${trackedRequests.length} sampled requests for completion (timeout=${pollTimeoutSec}s)...`);
      completionStats = await pollTrackedRequests();
    }
  }

  const report = buildReport(submissionElapsedSec, completionStats, batchResults);

  log('=== run complete ===');
  log(`submitted=${report.submission.submitted.toLocaleString()} acked=${report.submission.acked.toLocaleString()} ackFailed=${report.submission.ackFailed.toLocaleString()}`);
  log(`achievedSubmitRps=${report.submission.achievedSubmitRps} (configured=${rate})`);
  log(`ackLatencyMs p50=${report.submission.ackLatencyMs.p50} p95=${report.submission.ackLatencyMs.p95} p99=${report.submission.ackLatencyMs.p99}`);
  if (trackEnabled && batchSize > 1) {
    log(`batches: submitted=${report.batches.submitted} completed=${report.batches.completed} timedOut=${report.batches.timedOut}`);
    log(`batch requests: total=${report.batches.totalRequests} successful=${report.batches.successful} failed=${report.batches.failed} successRate=${report.batches.successRate}%`);
    log(`callback: delivered=${report.batches.callback.delivered} failed=${report.batches.callback.failed} pending=${report.batches.callback.pending} avgAttempts=${report.batches.callback.avgAttempts}`);
  } else if (trackEnabled) {
    log(`completion (sampled n=${report.completion.sampleSize}): completed=${report.completion.completed} successful=${report.completion.successful} failed=${report.completion.failed} expired=${report.completion.expired} rejected=${report.completion.rejected} timedOut=${report.completion.timedOut}`);
    log(`e2e latency ms (sampled): p50=${report.completion.e2eLatencyMs.p50} p95=${report.completion.e2eLatencyMs.p95} p99=${report.completion.e2eLatencyMs.p99}`);
  }
  for (const [model, stats] of Object.entries(report.perModelSubmitted)) {
    log(`model=${model} submitted=${stats.submitted} observedMaxRpm60s=${stats.observedMaxRpm60s} observedMaxTpm60s=${stats.observedMaxTpm60s}`);
  }

  console.log(JSON.stringify(report, null, 2));
}

main().catch((err) => {
  console.error('[loadgen] ERROR', err);
  process.exit(1);
});
