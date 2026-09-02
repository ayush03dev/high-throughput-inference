import express from 'express';

const app = express();
app.use(express.json());

const rejectAttempts = parseInt(process.env.REJECT_ATTEMPTS || '2', 10);
let attemptCount = 0;
const received = [];

app.post('/callback', (req, res) => {
  attemptCount += 1;
  received.push({ attempt: attemptCount, body: req.body, at: new Date().toISOString() });
  const batchId = req.body?.batchId ?? 'unknown';
  if (attemptCount <= rejectAttempts) {
    console.log(`[webhook-mock] REJECT batch=${batchId} attempt=${attemptCount}/${rejectAttempts + 1}`);
    return res.status(500).json({ error: 'simulated callback failure', attempt: attemptCount });
  }
  console.log(`[webhook-mock] ACCEPT batch=${batchId} attempt=${attemptCount} total=${req.body?.total} succeeded=${req.body?.succeeded}`);
  return res.status(200).json({ status: 'ok', attempt: attemptCount });
});

app.get('/received', (_req, res) => {
  res.json({ count: received.length, received });
});

app.get('/health', (_req, res) => res.json({ status: 'ok' }));

app.post('/reset', (_req, res) => {
  attemptCount = 0;
  received.length = 0;
  res.json({ status: 'reset' });
});

const port = process.env.PORT || 9000;
app.listen(port, () => {
  console.log(`webhook-mock listening on ${port}, rejectAttempts=${rejectAttempts}`);
});
