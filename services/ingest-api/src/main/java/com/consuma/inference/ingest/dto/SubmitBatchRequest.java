package com.consuma.inference.ingest.dto;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;

import java.util.List;

public record SubmitBatchRequest(
        @NotBlank String callbackUrl,
        @NotEmpty List<@Valid SubmitInferenceRequest> requests
) {
}
