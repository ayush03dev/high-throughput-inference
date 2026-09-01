package com.consuma.inference.scheduler;

import com.consuma.inference.common.config.CommonInfrastructureConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@Import(CommonInfrastructureConfig.class)
@EnableScheduling
public class SchedulerWorkerApplication {
    public static void main(String[] args) {
        SpringApplication.run(SchedulerWorkerApplication.class, args);
    }
}
