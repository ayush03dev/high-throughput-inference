package com.consuma.inference.scheduler.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

@Component
public class ProviderClient {

    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final String providerUrl;

    public ProviderClient(ObjectMapper objectMapper, @Value("${inference.provider-url}") String providerUrl) {
        this.objectMapper = objectMapper;
        this.providerUrl = providerUrl;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
    }

    public ProviderResult invoke(String requestId, String model, int estimatedTokens, JsonNode payload) {
        try {
            ObjectNode body = objectMapper.createObjectNode();
            body.put("requestId", requestId);
            body.put("model", model);
            body.put("estimatedTokens", estimatedTokens);
            body.set("payload", payload == null ? objectMapper.createObjectNode() : payload);

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(providerUrl + "/v1/inference"))
                    .timeout(Duration.ofSeconds(30))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(body)))
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() >= 200 && response.statusCode() < 300) {
                JsonNode result = objectMapper.readTree(response.body()).path("result");
                return ProviderResult.success(result);
            }
            return ProviderResult.failure("Provider returned status " + response.statusCode());
        } catch (Exception e) {
            return ProviderResult.failure(e.getMessage());
        }
    }

    public record ProviderResult(boolean success, JsonNode result, String error) {
        public static ProviderResult success(JsonNode result) {
            return new ProviderResult(true, result, null);
        }

        public static ProviderResult failure(String error) {
            return new ProviderResult(false, null, error);
        }
    }
}
