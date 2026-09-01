package com.consuma.inference.common.service;

import com.consuma.inference.common.domain.BatchStatus;
import com.consuma.inference.common.domain.RequestState;
import com.consuma.inference.common.entity.BatchEntity;
import com.consuma.inference.common.entity.RequestEntity;
import com.consuma.inference.common.repository.BatchRepository;
import com.consuma.inference.common.repository.RequestRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Optional;

@Service
public class BatchProgressService {

    private final BatchRepository batchRepository;
    private final RequestRepository requestRepository;

    public BatchProgressService(BatchRepository batchRepository, RequestRepository requestRepository) {
        this.batchRepository = batchRepository;
        this.requestRepository = requestRepository;
    }

    @Transactional
    public Optional<BatchEntity> recordTerminalRequest(RequestEntity request) {
        if (request.getBatchId() == null) {
            return Optional.empty();
        }
        BatchEntity batch = batchRepository.findByIdForUpdate(request.getBatchId()).orElse(null);
        if (batch == null) {
            return Optional.empty();
        }
        batch.setTerminalCount(batch.getTerminalCount() + 1);
        if (request.getState() == RequestState.SUCCEEDED) {
            batch.setSucceededCount(batch.getSucceededCount() + 1);
        } else if (request.getState() == RequestState.FAILED) {
            batch.setFailedCount(batch.getFailedCount() + 1);
        } else if (request.getState() == RequestState.EXPIRED) {
            batch.setExpiredCount(batch.getExpiredCount() + 1);
        }
        if (batch.getStatus() == BatchStatus.ACCEPTED) {
            batch.setStatus(BatchStatus.PROCESSING);
        }
        if (batch.getTerminalCount() >= batch.getTotalRequests()) {
            batch.setStatus(BatchStatus.COMPLETED);
            batch.setCompletedAt(Instant.now());
        }
        return Optional.of(batchRepository.save(batch));
    }

    public Optional<BatchEntity> findBatch(String batchId) {
        return batchRepository.findById(batchId);
    }
}
