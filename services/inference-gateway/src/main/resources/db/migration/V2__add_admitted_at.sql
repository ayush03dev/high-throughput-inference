-- Records the moment a request actually passed the rate limiter (set in
-- RequestProcessor.markInFlight), distinct from submitted_at (when the
-- client posted it) and completed_at (when the provider call finished).
-- Lets external validation measure the RPM/TPM sliding window against the
-- same instant the limiter actually enforced against, instead of using
-- completed_at as an approximation — completion can lag admission by tens
-- of seconds under saturating load, which let a handful of requests drift
-- across a 60s measurement boundary that admission itself never crossed.
ALTER TABLE requests ADD COLUMN admitted_at TIMESTAMPTZ;
