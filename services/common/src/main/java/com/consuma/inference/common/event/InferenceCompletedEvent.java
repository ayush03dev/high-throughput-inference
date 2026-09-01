package com.consuma.inference.common.event;

import com.consuma.inference.common.domain.RequestState;

public record InferenceCompletedEvent(
        String requestId,
        String batchId,
        String model,
        RequestState finalState
) {
}
