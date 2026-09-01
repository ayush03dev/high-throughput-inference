package com.consuma.inference.scheduler.service;

import com.consuma.inference.common.domain.RequestState;
import com.consuma.inference.common.entity.RequestEntity;
import com.consuma.inference.common.event.InferenceRequestEvent;
import com.consuma.inference.common.ratelimit.SlidingWindowRateLimiter;
import com.consuma.inference.common.repository.RequestRepository;
import com.consuma.inference.common.service.BatchProgressService;
import com.consuma.inference.common.service.ModelConfigService;
import com.consuma.inference.scheduler.client.ProviderClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Instant;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class RequestProcessorTest {

    @Mock
    private RequestRepository requestRepository;
    @Mock
    private SlidingWindowRateLimiter rateLimiter;
    @Mock
    private ModelConfigService modelConfigService;
    @Mock
    private ProviderClient providerClient;
    @Mock
    private BatchProgressService batchProgressService;
    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;

    private RequestProcessor processor;

    @BeforeEach
    void setUp() {
        processor = new RequestProcessor(
                requestRepository, rateLimiter, modelConfigService, providerClient,
                batchProgressService, kafkaTemplate, 300_000
        );
    }

    @Test
    void retriesWhenRateLimited() {
        RequestEntity entity = queuedEntity();
        InferenceRequestEvent event = event();
        when(requestRepository.findById("req-1")).thenReturn(Optional.of(entity));
        when(modelConfigService.getLimits("model-a")).thenReturn(new ModelConfigService.ModelLimits(100, 1000));
        when(rateLimiter.tryAcquire(anyString(), anyString(), anyInt(), anyLong(), anyLong())).thenReturn(false);

        assertEquals(RequestProcessor.ProcessResult.RETRY, processor.process(event));
        verify(providerClient, never()).invoke(anyString(), anyString(), anyInt(), any());
    }

    @Test
    void completesSuccessfulRequest() {
        RequestEntity entity = queuedEntity();
        InferenceRequestEvent event = event();
        when(requestRepository.findById("req-1")).thenReturn(Optional.of(entity));
        when(modelConfigService.getLimits("model-a")).thenReturn(new ModelConfigService.ModelLimits(100, 1000));
        when(rateLimiter.tryAcquire(anyString(), anyString(), anyInt(), anyLong(), anyLong())).thenReturn(true);
        when(providerClient.invoke(anyString(), anyString(), anyInt(), any()))
                .thenReturn(ProviderClient.ProviderResult.success(new ObjectMapper().createObjectNode()));
        when(requestRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));
        when(batchProgressService.recordTerminalRequest(any())).thenReturn(Optional.empty());

        assertEquals(RequestProcessor.ProcessResult.COMPLETED, processor.process(event));
        assertEquals(RequestState.SUCCEEDED, entity.getState());
    }

    private RequestEntity queuedEntity() {
        RequestEntity entity = new RequestEntity();
        entity.setRequestId("req-1");
        entity.setModel("model-a");
        entity.setEstimatedTokens(10);
        entity.setState(RequestState.QUEUED);
        entity.setSubmittedAt(Instant.now());
        return entity;
    }

    private InferenceRequestEvent event() {
        return new InferenceRequestEvent("req-1", null, "model-a", 10, new ObjectMapper().createObjectNode());
    }
}
