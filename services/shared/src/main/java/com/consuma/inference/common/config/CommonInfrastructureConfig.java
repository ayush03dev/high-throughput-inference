package com.consuma.inference.common.config;

import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@Configuration
@EnableJpaRepositories(basePackages = "com.consuma.inference.common.repository")
@EntityScan(basePackages = "com.consuma.inference.common.entity")
@ComponentScan(basePackages = "com.consuma.inference.common")
public class CommonInfrastructureConfig {
}
