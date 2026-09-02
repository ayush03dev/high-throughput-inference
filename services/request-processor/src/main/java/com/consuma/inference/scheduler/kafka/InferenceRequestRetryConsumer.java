package com.consuma.inference.scheduler.kafka;

import com.consuma.inference.common.event.InferenceRequestEvent;
import com.consuma.inference.scheduler.service.RequestProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class InferenceRequestRetryConsumer {

    private static final Logger log = LoggerFactory.getLogger(InferenceRequestRetryConsumer.class);

    private final RequestProcessor requestProcessor;
    private final InferenceRetryPublisher retryPublisher;
    private final long retryDelayMs;

    public InferenceRequestRetryConsumer(
            RequestProcessor requestProcessor,
            InferenceRetryPublisher retryPublisher,
            @Value("${inference.kafka.retry-delay-ms:100}") long retryDelayMs
    ) {
        this.requestProcessor = requestProcessor;
        this.retryPublisher = retryPublisher;
        this.retryDelayMs = retryDelayMs;
    }

    @KafkaListener(
            topics = "${inference.kafka.retry-topic:inference.requests.retry}",
            groupId = "${inference.kafka.retry-consumer-group:request-processors-retry}",
            containerFactory = "manualAckKafkaListenerContainerFactory"
    )
    public void consume(InferenceRequestEvent event, Acknowledgment acknowledgment) {
        try {
            if (retryDelayMs > 0) {
                Thread.sleep(retryDelayMs);
            }
            RequestProcessor.ProcessResult result = requestProcessor.process(event);
            if (result == RequestProcessor.ProcessResult.RETRY) {
                retryPublisher.publish(event);
            }
            acknowledgment.acknowledge();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted while waiting to retry request {}", event.requestId());
            acknowledgment.nack(java.time.Duration.ofSeconds(1));
        } catch (Exception e) {
            log.error("Failed to process retry for request {}", event.requestId(), e);
            acknowledgment.nack(java.time.Duration.ofSeconds(1));
        }
    }
}
