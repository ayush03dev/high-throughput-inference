package com.consuma.inference.common.kafka;

public final class KafkaTopics {
    public static final String INFERENCE_REQUESTS = "inference.requests";
    public static final String INFERENCE_REQUESTS_RETRY = "inference.requests.retry";
    public static final String INFERENCE_COMPLETED = "inference.completed";
    public static final String BATCH_CALLBACKS = "batch.callbacks";
    public static final String INFERENCE_DLQ = "inference.dlq";

    private KafkaTopics() {
    }
}
