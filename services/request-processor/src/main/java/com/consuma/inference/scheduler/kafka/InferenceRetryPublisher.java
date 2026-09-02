package com.consuma.inference.scheduler.kafka;

import com.consuma.inference.common.event.InferenceRequestEvent;
import com.consuma.inference.common.kafka.KafkaTopics;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class InferenceRetryPublisher {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public InferenceRetryPublisher(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publish(InferenceRequestEvent event) {
        kafkaTemplate.send(KafkaTopics.INFERENCE_REQUESTS_RETRY, event.requestId(), event);
    }
}
