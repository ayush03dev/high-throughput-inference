package com.consuma.inference.provider.service;

import com.consuma.inference.provider.config.ProviderProperties;
import com.consuma.inference.provider.dto.InferenceRequest;
import com.consuma.inference.provider.dto.InferenceResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class InferenceSimulationServiceTest {

    @Test
    void returnsSuccessWithZeroFailureRate() throws Exception {
        ProviderProperties props = new ProviderProperties();
        props.setDefaultLatencyMs(0);
        props.setFailureRate(0.0);
        InferenceSimulationService service = new InferenceSimulationService(props, new ObjectMapper());
        InferenceResponse response = service.simulate(
                new InferenceRequest("r1", "model-a", 100, new ObjectMapper().createObjectNode())
        );
        assertEquals("succeeded", response.status());
    }

    @Test
    void returnsFailureWithFullFailureRate() throws Exception {
        ProviderProperties props = new ProviderProperties();
        props.setDefaultLatencyMs(0);
        props.setFailureRate(1.0);
        InferenceSimulationService service = new InferenceSimulationService(props, new ObjectMapper());
        InferenceResponse response = service.simulate(
                new InferenceRequest("r1", "model-a", 100, new ObjectMapper().createObjectNode())
        );
        assertEquals("failed", response.status());
    }
}
