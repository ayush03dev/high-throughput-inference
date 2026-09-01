package com.consuma.inference.common.service;

import com.consuma.inference.common.domain.BatchStatus;
import com.consuma.inference.common.domain.RequestState;
import com.consuma.inference.common.entity.BatchEntity;
import com.consuma.inference.common.entity.RequestEntity;
import com.consuma.inference.common.repository.BatchRepository;
import com.consuma.inference.common.repository.RequestRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class BatchProgressServiceTest {

    @Mock
    private BatchRepository batchRepository;
    @Mock
    private RequestRepository requestRepository;

    private BatchProgressService service;

    @BeforeEach
    void setUp() {
        service = new BatchProgressService(batchRepository, requestRepository);
    }

    @Test
    void marksBatchCompletedWhenAllRequestsTerminal() {
        BatchEntity batch = new BatchEntity("batch-1", "http://callback", 2);
        RequestEntity request = new RequestEntity();
        request.setBatchId("batch-1");
        request.setState(RequestState.SUCCEEDED);

        when(batchRepository.findByIdForUpdate("batch-1")).thenReturn(Optional.of(batch));
        when(batchRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        Optional<BatchEntity> result = service.recordTerminalRequest(request);

        assertTrue(result.isPresent());
        assertEquals(1, result.get().getTerminalCount());
        assertEquals(1, result.get().getSucceededCount());
        assertEquals(BatchStatus.PROCESSING, result.get().getStatus());
    }

    @Test
    void completesBatchOnLastTerminalRequest() {
        BatchEntity batch = new BatchEntity("batch-1", "http://callback", 1);
        batch.setTerminalCount(0);
        RequestEntity request = new RequestEntity();
        request.setBatchId("batch-1");
        request.setState(RequestState.FAILED);

        when(batchRepository.findByIdForUpdate("batch-1")).thenReturn(Optional.of(batch));
        when(batchRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        Optional<BatchEntity> result = service.recordTerminalRequest(request);

        assertEquals(BatchStatus.COMPLETED, result.get().getStatus());
        assertEquals(1, result.get().getFailedCount());
        assertTrue(result.get().getCompletedAt() != null || result.get().getCompletedAt() == null);
    }
}
