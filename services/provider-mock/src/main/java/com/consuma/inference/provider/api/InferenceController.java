package com.consuma.inference.provider.api;

import com.consuma.inference.provider.dto.InferenceRequest;
import com.consuma.inference.provider.dto.InferenceResponse;
import com.consuma.inference.provider.service.InferenceSimulationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/v1")
public class InferenceController {

    private static final Logger log = LoggerFactory.getLogger(InferenceController.class);
    private static final long LOG_EVERY = 100;

    private final InferenceSimulationService simulationService;
    private final java.util.concurrent.atomic.AtomicLong requestCounter = new java.util.concurrent.atomic.AtomicLong();

    public InferenceController(InferenceSimulationService simulationService) {
        this.simulationService = simulationService;
    }

    @PostMapping("/inference")
    public ResponseEntity<InferenceResponse> infer(@RequestBody InferenceRequest request) throws InterruptedException {
        InferenceResponse response = simulationService.simulate(request);
        long count = requestCounter.incrementAndGet();
        if (count == 1 || count % LOG_EVERY == 0) {
            log.info(
                    "[provider] inference request={} model={} status={} (total={})",
                    request.requestId(),
                    request.model(),
                    response.status(),
                    count
            );
        }
        if ("failed".equals(response.status())) {
            return ResponseEntity.status(500).body(response);
        }
        return ResponseEntity.ok(response);
    }
}
