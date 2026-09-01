package com.consuma.inference.ingest;

import com.consuma.inference.common.config.CommonInfrastructureConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@Import(CommonInfrastructureConfig.class)
@EnableAsync
public class IngestApiApplication {
    public static void main(String[] args) {
        SpringApplication.run(IngestApiApplication.class, args);
    }
}
