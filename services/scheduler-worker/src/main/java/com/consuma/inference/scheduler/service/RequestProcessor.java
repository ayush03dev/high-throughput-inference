package com.consuma.inference.scheduler.service;

import com.consuma.inference.common.domain.BatchStatus;
import com.consuma.inference.common.domain.RequestState;
import com.consuma.inference.common.entity.BatchEntity;
import com.consuma.inference.common.entity.RequestEntity;
import com.consuma.inference.common.event.BatchCallbackEvent;
import com.consuma.inference.common.event.InferenceCompletedEvent;
import com.consuma.inference.common.event.InferenceRequestEvent;
import com.consuma.inference.common.kafka.KafkaTopics;
import com.consuma.inference.common.ratelimit.SlidingWindowRateLimiter;
import com.consuma.inference.common.repository.RequestRepository;
import com.consuma.inference.common.service.BatchProgressService;
import com.consuma.inference.common.service.ModelConfigService;
import com.consuma.inference.scheduler.client.ProviderClient;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

@Service
public class RequestProcessor {

    private final RequestRepository requestRepository;
    private final SlidingWindowRateLimiter rateLimiter;
    private final ModelConfigService modelConfigService;
    private final ProviderClient providerClient;
    private final BatchProgressService batchProgressService;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final long maxQueueWaitMs;

    public RequestProcessor(
            RequestRepository requestRepository,
            SlidingWindowRateLimiter rateLimiter,
            ModelConfigService modelConfigService,
            ProviderClient providerClient,
            BatchProgressService batchProgressService,
            KafkaTemplate<String, Object> kafkaTemplate,
            @Value("${inference.max-queue-wait-ms:300000}") long maxQueueWaitMs
    ) {
        this.requestRepository = requestRepository;
        this.rateLimiter = rateLimiter;
        this.modelConfigService = modelConfigService;
        this.providerClient = providerClient;
        this.batchProgressService = batchProgressService;
        this.kafkaTemplate = kafkaTemplate;
        this.maxQueueWaitMs = maxQueueWaitMs;
    }

    @Transactional
    public ProcessResult process(InferenceRequestEvent event) {
        RequestEntity request = requestRepository.findById(event.requestId()).orElse(null);
        if (request == null) {
            return ProcessResult.SKIPPED;
        }
        if (request.getState() != RequestState.QUEUED) {
            return ProcessResult.SKIPPED;
        }
        if (isExpired(request)) {
            markExpired(request);
            return ProcessResult.COMPLETED;
        }

        ModelConfigService.ModelLimits limits = modelConfigService.getLimits(event.model());
        if (!rateLimiter.tryAcquire(event.model(), event.requestId(), event.estimatedTokens(),
                limits.rpmLimit(), limits.tpmLimit())) {
            return ProcessResult.RETRY;
        }

        request.setState(RequestState.IN_FLIGHT);
        requestRepository.save(request);

        ProviderClient.ProviderResult result = providerClient.invoke(
                event.requestId(), event.model(), event.estimatedTokens(), event.payload()
        );

        if (result.success()) {
            request.setState(RequestState.SUCCEEDED);
            request.setResult(result.result());
        } else {
            request.setState(RequestState.FAILED);
            request.setErrorMessage(result.error());
        }
        request.setCompletedAt(Instant.now());
        requestRepository.save(request);

        kafkaTemplate.send(
                KafkaTopics.INFERENCE_COMPLETED,
                event.requestId(),
                new InferenceCompletedEvent(event.requestId(), event.batchId(), event.model(), request.getState())
        );

        batchProgressService.recordTerminalRequest(request).ifPresent(this::maybePublishCallback);
        return ProcessResult.COMPLETED;
    }

    private boolean isExpired(RequestEntity request) {
        return request.getSubmittedAt().plus(maxQueueWaitMs, ChronoUnit.MILLIS).isBefore(Instant.now());
    }

    private void markExpired(RequestEntity request) {
        request.setState(RequestState.EXPIRED);
        request.setCompletedAt(Instant.now());
        requestRepository.save(request);
        batchProgressService.recordTerminalRequest(request).ifPresent(this::maybePublishCallback);
    }

    private void maybePublishCallback(BatchEntity batch) {
        if (batch.getStatus() == BatchStatus.COMPLETED) {
            kafkaTemplate.send(
                    KafkaTopics.BATCH_CALLBACKS,
                    batch.getBatchId(),
                    new BatchCallbackEvent(batch.getBatchId(), batch.getCallbackUrl())
            );
        }
    }

    public enum ProcessResult {
        COMPLETED, RETRY, SKIPPED
    }
}
