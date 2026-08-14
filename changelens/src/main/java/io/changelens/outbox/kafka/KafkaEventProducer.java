package io.changelens.outbox.kafka;

import io.changelens.outbox.entity.OutboxEventEntity;

public interface KafkaEventProducer {

    void publish(OutboxEventEntity event) throws KafkaPublishException;
}
