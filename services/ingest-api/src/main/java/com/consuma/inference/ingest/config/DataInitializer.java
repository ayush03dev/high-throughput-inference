package com.consuma.inference.ingest.config;

import com.consuma.inference.common.entity.ModelEntity;
import com.consuma.inference.common.repository.ModelRepository;
import com.consuma.inference.common.service.ModelConfigService;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DataInitializer {

    @Bean
    CommandLineRunner seedModels(ModelRepository modelRepository, ModelConfigService modelConfigService) {
        return args -> {
            seedIfMissing(modelRepository, modelConfigService, "model-a", 50_000, 100_000_000);
            seedIfMissing(modelRepository, modelConfigService, "model-b", 20_000, 40_000_000);
        };
    }

    private void seedIfMissing(
            ModelRepository repository,
            ModelConfigService configService,
            String name,
            long rpm,
            long tpm
    ) {
        if (repository.findById(name).isEmpty()) {
            ModelEntity model = repository.save(new ModelEntity(name, rpm, tpm));
            configService.cacheLimits(model);
        } else {
            configService.cacheLimits(repository.findById(name).orElseThrow());
        }
    }
}
