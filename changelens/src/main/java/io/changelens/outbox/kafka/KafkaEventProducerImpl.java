package io.changelens.outbox.kafka;

import io.changelens.outbox.entity.OutboxEventEntity;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutionException;

@Component
@RequiredArgsConstructor
public class KafkaEventProducerImpl implements KafkaEventProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${changelens.kafka.topics.audit-events}")
    private String topic;

    @Override
    public void publish(OutboxEventEntity event) throws KafkaPublishException {

        try {
            kafkaTemplate
                    .send(topic, event.getEventId().toString(), event.getPayload())
                    .get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaPublishException("Kafka publishing interrupted", e);
        } catch (ExecutionException e) {
            throw new KafkaPublishException("Failed to publish audit event", e);
        }
    }
}
