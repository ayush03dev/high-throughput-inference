package com.consuma.inference.provider.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.HashMap;
import java.util.Map;

@ConfigurationProperties(prefix = "provider")
public class ProviderProperties {

    private long defaultLatencyMs = 10;
    private double failureRate = 0.0;
    private Map<String, ModelConfig> models = new HashMap<>();

    public long getDefaultLatencyMs() {
        return defaultLatencyMs;
    }

    public void setDefaultLatencyMs(long defaultLatencyMs) {
        this.defaultLatencyMs = defaultLatencyMs;
    }

    public double getFailureRate() {
        return failureRate;
    }

    public void setFailureRate(double failureRate) {
        this.failureRate = failureRate;
    }

    public Map<String, ModelConfig> getModels() {
        return models;
    }

    public void setModels(Map<String, ModelConfig> models) {
        this.models = models;
    }

    public static class ModelConfig {
        private long latencyMs = 10;
        private double failureRate = 0.0;

        public long getLatencyMs() {
            return latencyMs;
        }

        public void setLatencyMs(long latencyMs) {
            this.latencyMs = latencyMs;
        }

        public double getFailureRate() {
            return failureRate;
        }

        public void setFailureRate(double failureRate) {
            this.failureRate = failureRate;
        }
    }
}
