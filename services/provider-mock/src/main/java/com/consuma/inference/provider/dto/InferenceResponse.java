package com.consuma.inference.provider.dto;

import com.fasterxml.jackson.databind.JsonNode;

public record InferenceResponse(
        String requestId,
        String status,
        JsonNode result,
        String error
) {
    public static InferenceResponse success(String requestId, JsonNode result) {
        return new InferenceResponse(requestId, "succeeded", result, null);
    }

    public static InferenceResponse failure(String requestId, String error) {
        return new InferenceResponse(requestId, "failed", null, error);
    }
}
