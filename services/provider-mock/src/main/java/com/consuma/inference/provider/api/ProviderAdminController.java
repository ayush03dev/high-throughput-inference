package com.consuma.inference.provider.api;

import com.consuma.inference.provider.config.ProviderProperties;
import com.consuma.inference.provider.dto.UpdateFailureRateRequest;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

// Lets a reviewer/test harness change a model's simulated failure rate at
// runtime, the same way inference-gateway's admin endpoints change RPM/TPM
// limits live — otherwise failure-rate is only settable via env vars at
// container startup, which validate.py's scenarios can't reach without a
// restart.
@RestController
@RequestMapping("/v1/admin")
public class ProviderAdminController {

    private final ProviderProperties properties;

    public ProviderAdminController(ProviderProperties properties) {
        this.properties = properties;
    }

    @GetMapping("/models")
    public Map<String, ProviderProperties.ModelConfig> listModels() {
        return properties.getModels();
    }

    @PutMapping("/models/{model}/failure-rate")
    public ProviderProperties.ModelConfig updateFailureRate(
            @PathVariable("model") String model,
            @RequestBody UpdateFailureRateRequest request
    ) {
        ProviderProperties.ModelConfig config = properties.getModels()
                .computeIfAbsent(model, ignored -> new ProviderProperties.ModelConfig());
        config.setFailureRate(request.failureRate());
        return config;
    }
}
