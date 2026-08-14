package io.changelens.outbox;

import io.changelens.core.domain.audit.AuditEventType;
import io.changelens.core.enums.OutboxEventStatusType;
import io.changelens.outbox.entity.OutboxEventEntity;
import io.changelens.outbox.publisher.OutboxPublisher;
import io.changelens.outbox.repository.OutboxEventRepository;
import io.changelens.support.IntegrationTestContainers;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Import(IntegrationTestContainers.class)
class OutboxPublisherIntegrationTest {

    @Autowired
    private OutboxPublisher publisher;

    @Autowired
    private OutboxEventRepository repository;

    @Test
    void shouldPublishPendingEventToKafka() {

        OutboxEventEntity event =
                createPendingEvent();

        repository.save(event);

        publisher.publish(10);

        OutboxEventEntity persisted =
                repository.findById(event.getId()).orElseThrow();

        assertThat(persisted.getStatus())
                .isEqualTo(OutboxEventStatusType.PUBLISHED);

        assertThat(persisted.getPublishedAt())
                .isNotNull();

        assertThat(persisted.getAttempts())
                .isEqualTo(1);
    }

    private OutboxEventEntity createPendingEvent() {
        UUID eventId = UUID.randomUUID();

        return OutboxEventEntity.builder()
                .eventId(eventId)
                .aggregateType("AUDIT_EVENT")
                .aggregateId(eventId.toString())
                .eventType(AuditEventType.CREATE)
                .payload(Map.of(
                        "eventId", eventId.toString(),
                        "action", "CREATE"
                ))
                .status(OutboxEventStatusType.PENDING)
                .createdAt(Instant.now())
                .publishedAt(null)
                .attempts(0)
                .lastError(null)
                .updatedAt(Instant.now())
                .build();
    }
}
