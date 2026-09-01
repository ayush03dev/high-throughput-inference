package com.consuma.inference.ingest.api;

import com.consuma.inference.common.entity.BatchEntity;
import com.consuma.inference.common.entity.ModelEntity;
import com.consuma.inference.common.entity.RequestEntity;
import com.consuma.inference.common.repository.BatchRepository;
import com.consuma.inference.common.repository.RequestRepository;
import com.consuma.inference.common.service.ModelConfigService;
import com.consuma.inference.ingest.dto.*;
import com.consuma.inference.ingest.service.IngestService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;

@RestController
@RequestMapping("/v1")
public class IngestController {

    private final IngestService ingestService;
    private final RequestRepository requestRepository;
    private final BatchRepository batchRepository;
    private final ModelConfigService modelConfigService;

    public IngestController(
            IngestService ingestService,
            RequestRepository requestRepository,
            BatchRepository batchRepository,
            ModelConfigService modelConfigService
    ) {
        this.ingestService = ingestService;
        this.requestRepository = requestRepository;
        this.batchRepository = batchRepository;
        this.modelConfigService = modelConfigService;
    }

    @PostMapping("/inference")
    public ResponseEntity<AcceptedResponse> submitInference(@Valid @RequestBody SubmitInferenceRequest request) {
        RequestEntity entity = ingestService.submitSingle(request);
        return ResponseEntity.status(HttpStatus.ACCEPTED)
                .body(new AcceptedResponse(entity.getRequestId(), entity.getState().name().toLowerCase()));
    }

    @PostMapping("/batches")
    public ResponseEntity<BatchAcceptedResponse> submitBatch(@Valid @RequestBody SubmitBatchRequest request) {
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(ingestService.submitBatch(request));
    }

    @GetMapping("/requests/{requestId}")
    public RequestStatusResponse getRequest(@PathVariable("requestId") String requestId) {
        RequestEntity entity = requestRepository.findById(requestId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Request not found"));
        return new RequestStatusResponse(
                entity.getRequestId(),
                entity.getBatchId(),
                entity.getModel(),
                entity.getState(),
                entity.getResult(),
                entity.getErrorMessage(),
                entity.getSubmittedAt(),
                entity.getCompletedAt()
        );
    }

    @GetMapping("/batches/{batchId}")
    public BatchStatusResponse getBatch(@PathVariable("batchId") String batchId) {
        BatchEntity batch = batchRepository.findById(batchId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Batch not found"));
        return new BatchStatusResponse(
                batch.getBatchId(),
                batch.getStatus(),
                batch.getTotalRequests(),
                batch.getSucceededCount(),
                batch.getFailedCount(),
                batch.getExpiredCount(),
                batch.getCallbackStatus(),
                batch.getCallbackAttempts(),
                batch.getCreatedAt(),
                batch.getCompletedAt()
        );
    }

    @GetMapping("/admin/models")
    public List<ModelEntity> listModels() {
        return modelConfigService.findAll();
    }

    @PutMapping("/admin/models/{name}")
    public ModelEntity updateModel(@PathVariable("name") String name, @Valid @RequestBody UpdateModelLimitsRequest request) {
        return modelConfigService.updateLimits(name, request.rpmLimit(), request.tpmLimit());
    }
}
