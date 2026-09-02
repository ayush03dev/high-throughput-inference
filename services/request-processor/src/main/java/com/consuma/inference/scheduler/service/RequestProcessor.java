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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class RequestProcessor {

    private static final Logger log = LoggerFactory.getLogger(RequestProcessor.class);
    private static final long COMPLETED_LOG_EVERY = 100;
    private static final long RATE_LIMIT_LOG_EVERY = 50;

    private final AtomicLong completedCounter = new AtomicLong();
    private final ConcurrentHashMap<String, AtomicLong> rateLimitCounters = new ConcurrentHashMap<>();

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
            @Value("${inference.max-queue-wait-ms:1200000}") long maxQueueWaitMs
    ) {
        this.requestRepository = requestRepository;
        this.rateLimiter = rateLimiter;
        this.modelConfigService = modelConfigService;
        this.providerClient = providerClient;
        this.batchProgressService = batchProgressService;
        this.kafkaTemplate = kafkaTemplate;
        this.maxQueueWaitMs = maxQueueWaitMs;
    }

    public ProcessResult process(InferenceRequestEvent event) {
        RequestEntity request = requestRepository.findById(event.requestId()).orElse(null);
        if (request == null) {
            return ProcessResult.SKIPPED;
        }
        if (request.getState() != RequestState.QUEUED) {
            return ProcessResult.SKIPPED;
        }
        if (isExpired(request)) {
            expireRequest(request);
            return ProcessResult.COMPLETED;
        }

        ModelConfigService.ModelLimits limits = modelConfigService.getLimits(event.model());
        if (!rateLimiter.tryAcquire(event.model(), event.requestId(), event.estimatedTokens(),
                limits.rpmLimit(), limits.tpmLimit())) {
            logRateLimited(event.model(), event.requestId(), limits.rpmLimit(), limits.tpmLimit());
            return ProcessResult.RETRY;
        }

        if (!markInFlight(event.requestId())) {
            return ProcessResult.SKIPPED;
        }

        ProviderClient.ProviderResult result = providerClient.invoke(
                event.requestId(), event.model(), event.estimatedTokens(), event.payload()
        );

        finalizeRequest(event, result.success(), result.result(), result.error());
        logCompleted(event.requestId(), event.model(),
                result.success() ? RequestState.SUCCEEDED : RequestState.FAILED, event.batchId());
        return ProcessResult.COMPLETED;
    }

    @Transactional
    protected void expireRequest(RequestEntity request) {
        request.setState(RequestState.EXPIRED);
        request.setCompletedAt(Instant.now());
        requestRepository.save(request);
        batchProgressService.recordTerminalRequest(request).ifPresent(this::maybePublishCallback);
    }

    @Transactional
    protected boolean markInFlight(String requestId) {
        RequestEntity request = requestRepository.findById(requestId).orElse(null);
        if (request == null || request.getState() != RequestState.QUEUED) {
            return false;
        }
        request.setState(RequestState.IN_FLIGHT);
        request.setAdmittedAt(Instant.now());
        requestRepository.save(request);
        return true;
    }

    @Transactional
    protected void finalizeRequest(
            InferenceRequestEvent event,
            boolean success,
            JsonNode result,
            String error
    ) {
        RequestEntity request = requestRepository.findById(event.requestId()).orElse(null);
        if (request == null || request.getState() != RequestState.IN_FLIGHT) {
            return;
        }
        if (success) {
            request.setState(RequestState.SUCCEEDED);
            request.setResult(result);
        } else {
            request.setState(RequestState.FAILED);
            request.setErrorMessage(error);
        }
        request.setCompletedAt(Instant.now());
        requestRepository.save(request);

        kafkaTemplate.send(
                KafkaTopics.INFERENCE_COMPLETED,
                event.requestId(),
                new InferenceCompletedEvent(event.requestId(), event.batchId(), event.model(), request.getState())
        );

        batchProgressService.recordTerminalRequest(request).ifPresent(this::maybePublishCallback);
    }

    private void logRateLimited(String model, String requestId, long rpmLimit, long tpmLimit) {
        long count = rateLimitCounters.computeIfAbsent(model, ignored -> new AtomicLong()).incrementAndGet();
        if (count == 1 || count % RATE_LIMIT_LOG_EVERY == 0) {
            log.info(
                    "[processor] rate limited model={} request={} (rpmLimit={} tpmLimit={} throttled={})",
                    model,
                    requestId,
                    rpmLimit,
                    tpmLimit,
                    count
            );
        }
    }

    private void logCompleted(String requestId, String model, RequestState state, String batchId) {
        long count = completedCounter.incrementAndGet();
        if (count == 1 || count % COMPLETED_LOG_EVERY == 0) {
            log.info(
                    "[processor] completed request={} model={} state={} batch={} (total completed={})",
                    requestId,
                    model,
                    state,
                    batchId == null ? "-" : batchId,
                    count
            );
        }
    }

    private boolean isExpired(RequestEntity request) {
        return request.getSubmittedAt().plus(maxQueueWaitMs, ChronoUnit.MILLIS).isBefore(Instant.now());
    }

    private void maybePublishCallback(BatchEntity batch) {
        if (batch.getStatus() == BatchStatus.COMPLETED) {
            log.info(
                    "[processor] batch {} fully processed — enqueueing webhook delivery to {}",
                    batch.getBatchId(),
                    batch.getCallbackUrl()
            );
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
