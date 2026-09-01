package com.consuma.inference.scheduler.kafka;

import com.consuma.inference.common.event.InferenceRequestEvent;
import com.consuma.inference.scheduler.service.RequestProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class InferenceRequestConsumer {

    private static final Logger log = LoggerFactory.getLogger(InferenceRequestConsumer.class);

    private final RequestProcessor requestProcessor;

    public InferenceRequestConsumer(RequestProcessor requestProcessor) {
        this.requestProcessor = requestProcessor;
    }

    @KafkaListener(
            topics = "${inference.kafka.requests-topic:inference.requests}",
            groupId = "${inference.kafka.consumer-group:request-processors}",
            containerFactory = "manualAckKafkaListenerContainerFactory"
    )
    public void consume(InferenceRequestEvent event, Acknowledgment acknowledgment) {
        try {
            RequestProcessor.ProcessResult result = requestProcessor.process(event);
            if (result == RequestProcessor.ProcessResult.RETRY) {
                acknowledgment.nack(java.time.Duration.ofMillis(100));
                return;
            }
            acknowledgment.acknowledge();
        } catch (Exception e) {
            log.error("Failed to process request {}", event.requestId(), e);
            acknowledgment.nack(java.time.Duration.ofSeconds(1));
        }
    }
}
