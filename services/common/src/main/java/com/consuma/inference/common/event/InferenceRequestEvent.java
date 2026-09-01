package com.consuma.inference.common.event;

import com.fasterxml.jackson.databind.JsonNode;

public record InferenceRequestEvent(
        String requestId,
        String batchId,
        String model,
        int estimatedTokens,
        JsonNode payload
) {
}
