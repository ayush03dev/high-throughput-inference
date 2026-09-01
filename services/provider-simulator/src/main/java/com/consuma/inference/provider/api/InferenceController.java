package com.consuma.inference.provider.api;

import com.consuma.inference.provider.dto.InferenceRequest;
import com.consuma.inference.provider.dto.InferenceResponse;
import com.consuma.inference.provider.service.InferenceSimulationService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/v1")
public class InferenceController {

    private final InferenceSimulationService simulationService;

    public InferenceController(InferenceSimulationService simulationService) {
        this.simulationService = simulationService;
    }

    @PostMapping("/inference")
    public ResponseEntity<InferenceResponse> infer(@RequestBody InferenceRequest request) throws InterruptedException {
        InferenceResponse response = simulationService.simulate(request);
        if ("failed".equals(response.status())) {
            return ResponseEntity.status(500).body(response);
        }
        return ResponseEntity.ok(response);
    }
}
