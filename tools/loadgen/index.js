#!/usr/bin/env node
import { Command } from 'commander';
import { request, Agent } from 'undici';

const program = new Command();
program
  .requiredOption('--url <url>', 'Ingest API base URL')
  .option('--rate <n>', 'Requests per second', '100')
  .option('--duration <seconds>', 'Duration in seconds', '10')
  .option('--models <spec>', 'Model weights e.g. model-a:1.0', 'model-a:1.0')
  .option('--tokens <n>', 'Estimated tokens per request', '1000')
  .option('--batch-size <n>', 'Batch size (1 = single requests)', '1')
  .option('--callback-url <url>', 'Callback URL for batch mode')
  .parse();

const opts = program.opts();
const baseUrl = opts.url.replace(/\/$/, '');
const rate = parseInt(opts.rate, 10);
const durationSec = parseInt(opts.duration, 10);
const tokens = parseInt(opts.tokens, 10);
const batchSize = parseInt(opts.batchSize, 10);
const modelWeights = Object.fromEntries(
  opts.models.split(',').map((part) => {
    const [model, weight] = part.split(':');
    return [model.trim(), parseFloat(weight)];
  })
);

const agent = new Agent({ connections: 200, pipelining: 1 });
let submitted = 0;
let accepted = 0;
const latencies = [];

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

async function submitSingle(id) {
  const model = pickModel();
  const body = {
    requestId: id,
    model,
    estimatedTokens: tokens,
    payload: { prompt: `load-${id}` },
  };
  const start = Date.now();
  const res = await request(`${baseUrl}/v1/inference`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
    dispatcher: agent,
  });
  const text = await res.body.text();
  if (res.statusCode === 202) accepted += 1;
  latencies.push(Date.now() - start);
  return JSON.parse(text);
}

async function pollUntilDone(requestId, timeoutMs = 30000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const res = await request(`${baseUrl}/v1/requests/${requestId}`, { dispatcher: agent });
    const body = await res.body.json();
    if (['SUCCEEDED', 'FAILED', 'EXPIRED', 'REJECTED'].includes(body.state)) {
      return body;
    }
    await sleep(50);
  }
  return null;
}

async function submitBatch(batchIndex) {
  const requests = [];
  for (let i = 0; i < batchSize; i += 1) {
    const id = `batch-${batchIndex}-req-${i}`;
    requests.push({
      requestId: id,
      model: pickModel(),
      estimatedTokens: tokens,
      payload: { prompt: id },
    });
  }
  const start = Date.now();
  const res = await request(`${baseUrl}/v1/batches`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ callbackUrl: opts.callbackUrl, requests }),
    dispatcher: agent,
  });
  const body = await res.body.json();
  latencies.push(Date.now() - start);
  if (res.statusCode === 202) accepted += 1;
  submitted += requests.length;
  return body;
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

async function main() {
  const endAt = Date.now() + durationSec * 1000;
  let counter = 0;
  const intervalMs = Math.max(1, Math.floor(1000 / rate));

  if (batchSize > 1 && !opts.callbackUrl) {
    console.error('batch-size > 1 requires --callback-url');
    process.exit(1);
  }

  while (Date.now() < endAt) {
    const tickStart = Date.now();
    if (batchSize > 1) {
      await submitBatch(counter++);
    } else {
      const id = `req-${Date.now()}-${counter++}`;
      submitted += 1;
      await submitSingle(id);
    }
    const elapsed = Date.now() - tickStart;
    const wait = intervalMs - elapsed;
    if (wait > 0) await sleep(wait);
  }

  const report = {
    submitted,
    accepted,
    durationSec,
    configuredRate: rate,
    ackLatencyMs: {
      p50: percentile(latencies, 50),
      p95: percentile(latencies, 95),
      p99: percentile(latencies, 99),
    },
  };
  console.log(JSON.stringify(report, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
