package com.consuma.inference.callback.service;

import com.consuma.inference.callback.dto.CallbackPayload;
import com.consuma.inference.common.domain.BatchStatus;
import com.consuma.inference.common.domain.CallbackStatus;
import com.consuma.inference.common.entity.BatchEntity;
import com.consuma.inference.common.event.BatchCallbackEvent;
import com.consuma.inference.common.repository.BatchRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;

@Service
public class CallbackDeliveryService {

    private static final Logger log = LoggerFactory.getLogger(CallbackDeliveryService.class);
    private static final List<Long> BACKOFF_MS = List.of(1000L, 2000L, 4000L, 8000L, 16000L);

    private final BatchRepository batchRepository;
    private final ObjectMapper objectMapper;
    private final HttpClient httpClient;
    private final int maxAttempts;

    public CallbackDeliveryService(
            BatchRepository batchRepository,
            ObjectMapper objectMapper,
            @Value("${inference.callback.max-attempts:5}") int maxAttempts
    ) {
        this.batchRepository = batchRepository;
        this.objectMapper = objectMapper;
        this.maxAttempts = maxAttempts;
        this.httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    }

    @Transactional
    public boolean deliver(BatchCallbackEvent event) {
        BatchEntity batch = batchRepository.findById(event.batchId()).orElse(null);
        if (batch == null) {
            log.warn("Batch not found for callback: {}", event.batchId());
            return true;
        }
        if (batch.getCallbackStatus() == CallbackStatus.DELIVERED) {
            return true;
        }

        CallbackPayload payload = CallbackPayload.from(
                batch.getStatus(),
                batch.getBatchId(),
                batch.getTotalRequests(),
                batch.getSucceededCount(),
                batch.getFailedCount(),
                batch.getExpiredCount()
        );

        for (int attempt = 0; attempt < maxAttempts; attempt++) {
            batch.setCallbackAttempts(batch.getCallbackAttempts() + 1);
            batchRepository.save(batch);
            if (post(event.callbackUrl(), payload)) {
                batch.setCallbackStatus(CallbackStatus.DELIVERED);
                batchRepository.save(batch);
                return true;
            }
            sleep(BACKOFF_MS.get(Math.min(attempt, BACKOFF_MS.size() - 1)));
        }

        batch.setCallbackStatus(CallbackStatus.FAILED);
        batchRepository.save(batch);
        return false;
    }

    private boolean post(String callbackUrl, CallbackPayload payload) {
        try {
            String body = objectMapper.writeValueAsString(payload);
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(callbackUrl))
                    .timeout(Duration.ofSeconds(10))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            return response.statusCode() >= 200 && response.statusCode() < 300;
        } catch (Exception e) {
            log.warn("Callback delivery failed: {}", e.getMessage());
            return false;
        }
    }

    private void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
