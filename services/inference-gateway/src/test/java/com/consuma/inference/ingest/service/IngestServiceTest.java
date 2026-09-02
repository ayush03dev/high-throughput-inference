package com.consuma.inference.ingest.service;

import com.consuma.inference.common.domain.RequestState;
import com.consuma.inference.common.entity.ModelEntity;
import com.consuma.inference.common.entity.RequestEntity;
import com.consuma.inference.common.event.InferenceRequestEvent;
import com.consuma.inference.common.kafka.KafkaTopics;
import com.consuma.inference.common.repository.BatchRepository;
import com.consuma.inference.common.repository.RequestRepository;
import com.consuma.inference.common.service.ModelConfigService;
import com.consuma.inference.ingest.dto.SubmitInferenceRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class IngestServiceTest {

    @Mock
    private RequestRepository requestRepository;
    @Mock
    private BatchRepository batchRepository;
    @Mock
    private ModelConfigService modelConfigService;
    @Mock
    private KafkaTemplate<String, InferenceRequestEvent> kafkaTemplate;
    @Mock
    private AsyncBatchPersister asyncBatchPersister;

    private IngestService ingestService;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        ingestService = new IngestService(
                requestRepository, batchRepository, modelConfigService,
                kafkaTemplate, asyncBatchPersister
        );
        objectMapper = new ObjectMapper();
    }

    @Test
    void submitsNewRequestAndPublishesToKafka() {
        SubmitInferenceRequest request = new SubmitInferenceRequest(
                "req-1", "model-a", objectMapper.createObjectNode(), 100
        );
        when(requestRepository.findById("req-1")).thenReturn(Optional.empty());
        when(modelConfigService.findModel("model-a")).thenReturn(Optional.of(new ModelEntity("model-a", 1000, 10000)));
        when(requestRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        RequestEntity saved = ingestService.submitSingle(request);

        assertEquals(RequestState.QUEUED, saved.getState());
        verify(kafkaTemplate).send(eq(KafkaTopics.INFERENCE_REQUESTS), eq("req-1"), any(InferenceRequestEvent.class));
    }

    @Test
    void returnsExistingRequestForIdempotentSubmit() {
        RequestEntity existing = new RequestEntity();
        existing.setRequestId("req-1");
        existing.setState(RequestState.SUCCEEDED);
        when(requestRepository.findById("req-1")).thenReturn(Optional.of(existing));

        RequestEntity result = ingestService.submitSingle(
                new SubmitInferenceRequest("req-1", "model-a", objectMapper.createObjectNode(), 100)
        );

        assertEquals(RequestState.SUCCEEDED, result.getState());
        verify(kafkaTemplate, never()).send(anyString(), anyString(), any());
    }
}
