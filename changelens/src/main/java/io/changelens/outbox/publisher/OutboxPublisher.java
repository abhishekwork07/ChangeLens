package io.changelens.outbox.publisher;

import io.changelens.outbox.entity.OutboxEventEntity;
import io.changelens.outbox.kafka.KafkaEventProducer;
import io.changelens.outbox.publisher.service.OutboxClaimService;
import io.changelens.outbox.publisher.service.OutboxStatusService;
import lombok.RequiredArgsConstructor;

import java.time.Instant;
import java.util.List;

@RequiredArgsConstructor
public class OutboxPublisher {

    private final OutboxClaimService claimService;
    private final KafkaEventProducer kafkaEventProducer;
    private final OutboxStatusService statusService;

    public void publish(int batchSize) {

        List<OutboxEventEntity> events = claimService.claimPendingEvents(batchSize);

        for (OutboxEventEntity event : events) {
            publishEvent(event);
        }
    }

    private void publishEvent(OutboxEventEntity event) {
        try {
            kafkaEventProducer.publish(event);
            statusService.markPublished(event.getEventId(), Instant.now());

        } catch (Exception exception) {
            statusService.markFailed(event.getEventId(), exception.getMessage());
        }
    }
}
