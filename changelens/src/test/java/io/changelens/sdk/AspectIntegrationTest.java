package io.changelens.sdk;

import io.changelens.core.context.AuditContext;
import io.changelens.core.domain.actor.Actor;
import io.changelens.core.domain.actor.ActorType;
import io.changelens.core.domain.audit.AuditEvent;
import io.changelens.core.domain.audit.AuditEventType;
import io.changelens.core.domain.change.ChangeSet;
import io.changelens.core.domain.resource.Resource;
import io.changelens.core.enums.OutboxEventStatusType;
import io.changelens.outbox.entity.OutboxEventEntity;
import io.changelens.outbox.repository.OutboxEventRepository;
import io.changelens.sdk.audit.AuditEventPublisher;
import io.changelens.support.IntegrationTestContainers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import static org.assertj.core.api.Assertions.assertThat;


@SpringBootTest
@Import(IntegrationTestContainers.class)
public class AspectIntegrationTest {

    @Autowired
    private AuditEventPublisher publisher;

    @Autowired
    private OutboxEventRepository outboxEventRepository;


    @BeforeEach
    void setUp() {
        outboxEventRepository.deleteAll();
    }

    @Test
    void shouldPersistAuditEventToOutbox() {
        UUID eventId = UUID.randomUUID();

        AuditEvent event = createAuditEvent(eventId);

        publisher.publish(event);

        OutboxEventEntity saved =
                outboxEventRepository.findByEventId(eventId);

        assertThat(saved.getEventId())
                .isEqualTo(eventId);

        assertThat(saved.getAggregateType())
                .isEqualTo("AUDIT_EVENT");

        assertThat(saved.getAggregateId())
                .isEqualTo(eventId.toString());

        assertThat(saved.getEventType())
                .isEqualTo(event.eventType());

        assertThat(saved.getStatus())
                .isEqualTo(OutboxEventStatusType.PENDING);

        assertThat(saved.getAttempts())
                .isZero();

        assertThat(saved.getPayload())
                .isNotEmpty();
    }

    private AuditEvent createAuditEvent(UUID eventId) {

        return new AuditEvent(
                        eventId,
                        1,
                        "tenant-1",
                        AuditEventType.CREATE,
                        Instant.now(),
                        "CREATE",
                        new Actor(
                                ActorType.USER,
                                "user-1",
                                "Test User"
                        ),
                        new Resource(
                                "RESOURCE",
                                "resource-1",
                                "Test Resource"
                        ),
                        new ChangeSet(
                                "test summary",
                                List.of()
                        ),
                        null,
                        null,
                        new AuditContext(
                                "ChangeLens",
                                "1.0.0",
                                "audit-service",
                                "test",
                                "request-1",
                                "correlation-1",
                                "trace-1",
                                "127.0.0.1",
                                "JUnit"
                        ),
                        Map.of()
                );
    }
}
