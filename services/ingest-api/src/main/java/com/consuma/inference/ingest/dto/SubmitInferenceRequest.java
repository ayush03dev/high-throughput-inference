package com.consuma.inference.ingest.dto;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public record SubmitInferenceRequest(
        @NotBlank String requestId,
        @NotBlank String model,
        @NotNull JsonNode payload,
        @Min(1) int estimatedTokens
) {
}
