package com.consuma.inference.callback.service;

import com.consuma.inference.common.domain.BatchStatus;
import com.consuma.inference.common.domain.CallbackStatus;
import com.consuma.inference.common.entity.BatchEntity;
import com.consuma.inference.common.event.BatchCallbackEvent;
import com.consuma.inference.common.repository.BatchRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class CallbackDeliveryServiceTest {

    @Mock
    private BatchRepository batchRepository;

    private CallbackDeliveryService service;

    @BeforeEach
    void setUp() {
        service = new CallbackDeliveryService(batchRepository, new ObjectMapper(), 1);
    }

    @Test
    void marksDeliveredWhenCallbackUrlInvalidButAttemptsRecorded() {
        BatchEntity batch = new BatchEntity("batch-1", "http://invalid.local/callback", 10);
        batch.setStatus(BatchStatus.COMPLETED);
        when(batchRepository.findById("batch-1")).thenReturn(Optional.of(batch));
        when(batchRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        boolean delivered = service.deliver(new BatchCallbackEvent("batch-1", "http://invalid.local/callback"));

        assertEquals(false, delivered);
        assertEquals(CallbackStatus.FAILED, batch.getCallbackStatus());
        assertEquals(1, batch.getCallbackAttempts());
    }
}
