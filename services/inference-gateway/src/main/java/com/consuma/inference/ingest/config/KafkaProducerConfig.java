package com.consuma.inference.ingest.config;

import com.consuma.inference.common.event.InferenceRequestEvent;
import com.consuma.inference.common.kafka.KafkaTopics;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig {

    @Bean
    public ProducerFactory<String, InferenceRequestEvent> producerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers
    ) {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    public KafkaTemplate<String, InferenceRequestEvent> kafkaTemplate(
            ProducerFactory<String, InferenceRequestEvent> producerFactory
    ) {
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    public NewTopic inferenceRequestsTopic() {
        return new NewTopic(KafkaTopics.INFERENCE_REQUESTS, 8, (short) 1);
    }

    @Bean
    public NewTopic inferenceCompletedTopic() {
        return new NewTopic(KafkaTopics.INFERENCE_COMPLETED, 8, (short) 1);
    }

    @Bean
    public NewTopic batchCallbacksTopic() {
        return new NewTopic(KafkaTopics.BATCH_CALLBACKS, 4, (short) 1);
    }

    @Bean
    public NewTopic inferenceDlqTopic() {
        return new NewTopic(KafkaTopics.INFERENCE_DLQ, 4, (short) 1);
    }
}
