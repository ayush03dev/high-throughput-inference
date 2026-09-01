package com.consuma.inference.callback.dto;

import com.consuma.inference.common.domain.BatchStatus;
import com.consuma.inference.common.domain.CallbackStatus;

public record CallbackPayload(
        String batchId,
        String status,
        int total,
        int succeeded,
        int failed,
        int expired,
        String resultsUrl,
        Object results
) {
    public static CallbackPayload from(BatchStatus batchStatus, String batchId, int total, int succeeded, int failed, int expired) {
        return new CallbackPayload(
                batchId,
                batchStatus.name().toLowerCase(),
                total,
                succeeded,
                failed,
                expired,
                "/v1/batches/" + batchId + "/results",
                null
        );
    }
}
