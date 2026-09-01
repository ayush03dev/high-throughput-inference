package com.consuma.inference.callback;

import com.consuma.inference.common.config.CommonInfrastructureConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;

@SpringBootApplication
@Import(CommonInfrastructureConfig.class)
public class CallbackWorkerApplication {
    public static void main(String[] args) {
        SpringApplication.run(CallbackWorkerApplication.class, args);
    }
}
