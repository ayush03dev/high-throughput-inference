CREATE TABLE models (
    name VARCHAR(64) PRIMARY KEY,
    rpm_limit BIGINT NOT NULL,
    tpm_limit BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE batches (
    batch_id VARCHAR(128) PRIMARY KEY,
    callback_url VARCHAR(2048) NOT NULL,
    total_requests INT NOT NULL,
    terminal_count INT NOT NULL DEFAULT 0,
    succeeded_count INT NOT NULL DEFAULT 0,
    failed_count INT NOT NULL DEFAULT 0,
    expired_count INT NOT NULL DEFAULT 0,
    status VARCHAR(32) NOT NULL,
    callback_status VARCHAR(32),
    callback_attempts INT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ
);

CREATE TABLE requests (
    request_id VARCHAR(128) PRIMARY KEY,
    batch_id VARCHAR(128) REFERENCES batches(batch_id),
    model VARCHAR(64) NOT NULL REFERENCES models(name),
    estimated_tokens INT NOT NULL,
    state VARCHAR(32) NOT NULL,
    payload JSONB,
    result JSONB,
    submitted_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    error_message TEXT
);

CREATE INDEX idx_requests_batch_id ON requests(batch_id);
CREATE INDEX idx_requests_state ON requests(state);
CREATE INDEX idx_requests_model ON requests(model);
