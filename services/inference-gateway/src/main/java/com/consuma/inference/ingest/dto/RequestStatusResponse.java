package com.consuma.inference.ingest.dto;

import com.consuma.inference.common.domain.RequestState;
import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;

public record RequestStatusResponse(
        String requestId,
        String batchId,
        String model,
        RequestState state,
        JsonNode result,
        String errorMessage,
        Instant submittedAt,
        Instant admittedAt,
        Instant completedAt
) {
}
