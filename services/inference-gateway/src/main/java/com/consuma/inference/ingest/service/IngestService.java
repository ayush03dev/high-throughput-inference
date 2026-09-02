package com.consuma.inference.ingest.service;

import com.consuma.inference.common.domain.BatchStatus;
import com.consuma.inference.common.domain.RequestState;
import com.consuma.inference.common.entity.BatchEntity;
import com.consuma.inference.common.entity.RequestEntity;
import com.consuma.inference.common.event.InferenceRequestEvent;
import com.consuma.inference.common.kafka.KafkaTopics;
import com.consuma.inference.common.repository.BatchRepository;
import com.consuma.inference.common.repository.RequestRepository;
import com.consuma.inference.common.service.ModelConfigService;
import com.consuma.inference.ingest.dto.BatchAcceptedResponse;
import com.consuma.inference.ingest.dto.SubmitBatchRequest;
import com.consuma.inference.ingest.dto.SubmitInferenceRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class IngestService {

    private static final Logger log = LoggerFactory.getLogger(IngestService.class);
    private static final long SINGLE_REQUEST_LOG_EVERY = 100;

    private final AtomicLong singleRequestCounter = new AtomicLong();
    private final RequestRepository requestRepository;
    private final BatchRepository batchRepository;
    private final ModelConfigService modelConfigService;
    private final KafkaTemplate<String, InferenceRequestEvent> kafkaTemplate;
    private final AsyncBatchPersister asyncBatchPersister;

    public IngestService(
            RequestRepository requestRepository,
            BatchRepository batchRepository,
            ModelConfigService modelConfigService,
            KafkaTemplate<String, InferenceRequestEvent> kafkaTemplate,
            AsyncBatchPersister asyncBatchPersister
    ) {
        this.requestRepository = requestRepository;
        this.batchRepository = batchRepository;
        this.modelConfigService = modelConfigService;
        this.kafkaTemplate = kafkaTemplate;
        this.asyncBatchPersister = asyncBatchPersister;
    }

    @Transactional
    public RequestEntity submitSingle(SubmitInferenceRequest request) {
        Optional<RequestEntity> existing = requestRepository.findById(request.requestId());
        if (existing.isPresent()) {
            return existing.get();
        }
        validateModel(request.model());
        RequestEntity entity = buildRequest(request, null);
        requestRepository.save(entity);
        publish(entity);
        long accepted = singleRequestCounter.incrementAndGet();
        if (accepted == 1 || accepted % SINGLE_REQUEST_LOG_EVERY == 0) {
            log.info("[gateway] accepted request={} model={} (total accepted={})", entity.getRequestId(), entity.getModel(), accepted);
        }
        return entity;
    }

    @Transactional
    public BatchAcceptedResponse submitBatch(SubmitBatchRequest batchRequest) {
        List<SubmitInferenceRequest> requests = batchRequest.requests();
        Set<String> seen = new HashSet<>();
        Set<String> models = new HashSet<>();
        for (SubmitInferenceRequest req : requests) {
            if (!seen.add(req.requestId())) {
                throw new IllegalArgumentException("Duplicate request_id in batch: " + req.requestId());
            }
            models.add(req.model());
        }
        for (String model : models) {
            validateModel(model);
        }

        String batchId = "batch-" + UUID.randomUUID();
        Instant now = Instant.now();
        batchRepository.saveAndFlush(new BatchEntity(batchId, batchRequest.callbackUrl(), requests.size()));
        log.info(
                "[gateway] batch accepted batchId={} size={} callbackUrl={}",
                batchId,
                requests.size(),
                batchRequest.callbackUrl()
        );
        asyncBatchPersister.persistAndPublish(batchId, requests, now);

        return new BatchAcceptedResponse(batchId, BatchStatus.ACCEPTED.name().toLowerCase(), requests.size());
    }

    private void validateModel(String model) {
        modelConfigService.findModel(model)
                .orElseThrow(() -> new IllegalArgumentException("Unknown model: " + model));
    }

    private RequestEntity buildRequest(SubmitInferenceRequest request, String batchId) {
        RequestEntity entity = new RequestEntity();
        entity.setRequestId(request.requestId());
        entity.setBatchId(batchId);
        entity.setModel(request.model());
        entity.setEstimatedTokens(request.estimatedTokens());
        entity.setPayload(request.payload());
        entity.setState(RequestState.QUEUED);
        entity.setSubmittedAt(Instant.now());
        return entity;
    }

    private void publish(RequestEntity entity) {
        InferenceRequestEvent event = new InferenceRequestEvent(
                entity.getRequestId(),
                entity.getBatchId(),
                entity.getModel(),
                entity.getEstimatedTokens(),
                entity.getPayload()
        );
        kafkaTemplate.send(KafkaTopics.INFERENCE_REQUESTS, entity.getRequestId(), event);
    }
}
