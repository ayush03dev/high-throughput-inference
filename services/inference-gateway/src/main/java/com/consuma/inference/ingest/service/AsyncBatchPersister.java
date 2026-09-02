package com.consuma.inference.ingest.service;

import com.consuma.inference.common.event.InferenceRequestEvent;
import com.consuma.inference.common.kafka.KafkaTopics;
import com.consuma.inference.ingest.dto.SubmitInferenceRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

@Service
public class AsyncBatchPersister {

    private static final Logger log = LoggerFactory.getLogger(AsyncBatchPersister.class);
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
    public void persistAndPublish(String batchId, List<SubmitInferenceRequest> requests, Instant submittedAt) {
        long startMs = System.currentTimeMillis();
        batchJdbcInserter.insertAll(batchId, requests, submittedAt);
        log.info("[gateway] batch {}: {} requests written to Postgres", batchId, requests.size());

        List<InferenceRequestEvent> events = new ArrayList<>(requests.size());
        for (SubmitInferenceRequest req : requests) {
            events.add(new InferenceRequestEvent(
                    req.requestId(), batchId, req.model(), req.estimatedTokens(), req.payload()
            ));
        }
        for (int i = 0; i < events.size(); i++) {
            InferenceRequestEvent event = events.get(i);
            try {
                kafkaTemplate.send(KafkaTopics.INFERENCE_REQUESTS, event.requestId(), event).get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while publishing batch " + batchId, e);
            } catch (ExecutionException e) {
                throw new IllegalStateException("Failed to publish batch " + batchId + " to Kafka", e.getCause());
            }
            if ((i + 1) % PUBLISH_CHUNK_SIZE == 0) {
                kafkaTemplate.flush();
                log.info("[gateway] batch {}: {}/{} requests handed off to Kafka so far", batchId, i + 1, events.size());
            }
        }
        kafkaTemplate.flush();
        log.info(
                "[gateway] batch {} fully queued for processing — {} Kafka messages in {}ms",
                batchId,
                events.size(),
                System.currentTimeMillis() - startMs
        );
    }
}
