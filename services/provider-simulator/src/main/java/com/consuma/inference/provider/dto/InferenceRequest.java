package com.consuma.inference.provider.dto;

import com.fasterxml.jackson.databind.JsonNode;

public record InferenceRequest(
        String requestId,
        String model,
        int estimatedTokens,
        JsonNode payload
) {
}
