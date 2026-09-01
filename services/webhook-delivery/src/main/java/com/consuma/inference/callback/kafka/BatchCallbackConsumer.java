package com.consuma.inference.callback.kafka;

import com.consuma.inference.common.event.BatchCallbackEvent;
import com.consuma.inference.callback.service.CallbackDeliveryService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class BatchCallbackConsumer {

    private final CallbackDeliveryService deliveryService;

    public BatchCallbackConsumer(CallbackDeliveryService deliveryService) {
        this.deliveryService = deliveryService;
    }

    @KafkaListener(
            topics = "${inference.kafka.callbacks-topic:batch.callbacks}",
            groupId = "${inference.kafka.callback-consumer-group:webhook-delivery-workers}"
    )
    public void consume(BatchCallbackEvent event) {
        deliveryService.deliver(event);
    }
}
