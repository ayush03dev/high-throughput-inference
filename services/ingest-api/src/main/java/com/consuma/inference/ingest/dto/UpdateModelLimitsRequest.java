package com.consuma.inference.ingest.dto;

import jakarta.validation.constraints.Min;

public record UpdateModelLimitsRequest(
        @Min(1) long rpmLimit,
        @Min(1) long tpmLimit
) {
}
