package com.consuma.inference.provider.service;

import com.consuma.inference.provider.config.ProviderProperties;
import com.consuma.inference.provider.dto.InferenceRequest;
import com.consuma.inference.provider.dto.InferenceResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.stereotype.Service;

import java.util.concurrent.ThreadLocalRandom;

@Service
public class InferenceSimulationService {

    private final ProviderProperties properties;
    private final ObjectMapper objectMapper;

    public InferenceSimulationService(ProviderProperties properties, ObjectMapper objectMapper) {
        this.properties = properties;
        this.objectMapper = objectMapper;
    }

    public InferenceResponse simulate(InferenceRequest request) throws InterruptedException {
        ProviderProperties.ModelConfig modelConfig = properties.getModels()
                .getOrDefault(request.model(), new ProviderProperties.ModelConfig());
        long latency = modelConfig.getLatencyMs() > 0 ? modelConfig.getLatencyMs() : properties.getDefaultLatencyMs();
        if (latency > 0) {
            Thread.sleep(latency);
        }
        double failureRate = modelConfig.getFailureRate() > 0 ? modelConfig.getFailureRate() : properties.getFailureRate();
        if (ThreadLocalRandom.current().nextDouble() < failureRate) {
            return InferenceResponse.failure(request.requestId(), "simulated provider failure");
        }
        ObjectNode result = objectMapper.createObjectNode();
        result.put("model", request.model());
        result.put("tokens", request.estimatedTokens());
        result.put("output", "simulated-response");
        return InferenceResponse.success(request.requestId(), result);
    }
}
