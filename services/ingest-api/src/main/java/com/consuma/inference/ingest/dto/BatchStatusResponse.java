package com.consuma.inference.ingest.dto;

import com.consuma.inference.common.domain.BatchStatus;
import com.consuma.inference.common.domain.CallbackStatus;

import java.time.Instant;

public record BatchStatusResponse(
        String batchId,
        BatchStatus status,
        int total,
        int succeeded,
        int failed,
        int expired,
        CallbackStatus callbackStatus,
        int callbackAttempts,
        Instant createdAt,
        Instant completedAt
) {
}
