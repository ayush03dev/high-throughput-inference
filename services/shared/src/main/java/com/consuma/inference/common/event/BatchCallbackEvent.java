package com.consuma.inference.common.event;

public record BatchCallbackEvent(
        String batchId,
        String callbackUrl
) {
}
