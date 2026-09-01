package com.consuma.inference.provider.api;

import com.consuma.inference.provider.config.ProviderConfig;
import com.consuma.inference.provider.service.InferenceSimulationService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(InferenceController.class)
@Import({InferenceSimulationService.class, ProviderConfig.class, ObjectMapper.class})
class InferenceControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    void returns200OnSuccess() throws Exception {
        String body = """
                {"requestId":"r1","model":"model-a","estimatedTokens":100,"payload":{}}
                """;
        mockMvc.perform(post("/v1/inference").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("succeeded"));
    }
}
