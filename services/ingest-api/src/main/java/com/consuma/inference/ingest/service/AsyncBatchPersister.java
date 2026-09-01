package com.consuma.inference.ingest.service;

import com.consuma.inference.common.event.InferenceRequestEvent;
import com.consuma.inference.common.kafka.KafkaTopics;
import com.consuma.inference.ingest.dto.SubmitInferenceRequest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

@Service
public class AsyncBatchPersister {

    private static final int PUBLISH_CHUNK_SIZE = 250;

    private final BatchJdbcInserter batchJdbcInserter;
    private final KafkaTemplate<String, InferenceRequestEvent> kafkaTemplate;

    public AsyncBatchPersister(
            BatchJdbcInserter batchJdbcInserter,
            KafkaTemplate<String, InferenceRequestEvent> kafkaTemplate
    ) {
        this.batchJdbcInserter = batchJdbcInserter;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Async
    @Transactional
    public void persistAndPublish(String batchId, List<SubmitInferenceRequest> requests, Instant submittedAt) {
        batchJdbcInserter.insertAll(batchId, requests, submittedAt);

        List<InferenceRequestEvent> events = new ArrayList<>(requests.size());
        for (SubmitInferenceRequest req : requests) {
            events.add(new InferenceRequestEvent(
                    req.requestId(), batchId, req.model(), req.estimatedTokens(), req.payload()
            ));
        }
        for (int i = 0; i < events.size(); i++) {
            InferenceRequestEvent event = events.get(i);
            try {
                kafkaTemplate.send(KafkaTopics.INFERENCE_REQUESTS, event.model(), event).get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while publishing batch " + batchId, e);
            } catch (ExecutionException e) {
                throw new IllegalStateException("Failed to publish batch " + batchId + " to Kafka", e.getCause());
            }
            if ((i + 1) % PUBLISH_CHUNK_SIZE == 0) {
                kafkaTemplate.flush();
            }
        }
        kafkaTemplate.flush();
    }
}
