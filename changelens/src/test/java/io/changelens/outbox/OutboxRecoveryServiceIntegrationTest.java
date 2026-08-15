package io.changelens.outbox;

import io.changelens.core.domain.audit.AuditEventType;
import io.changelens.core.enums.OutboxEventStatusType;
import io.changelens.outbox.entity.OutboxEventEntity;
import io.changelens.outbox.publisher.service.OutboxRecoveryService;
import io.changelens.outbox.repository.OutboxEventRepository;
import io.changelens.support.IntegrationTestContainers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Import(IntegrationTestContainers.class)
class OutboxRecoveryServiceIntegrationTest {

    @Autowired
    private OutboxRecoveryService recoveryService;

    @Autowired
    private OutboxEventRepository repository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    void setUp() {
        repository.deleteAll();
    }

    @Test
    void shouldRecoverStaleProcessingEvent() {

        // Given
        OutboxEventEntity event =
                createProcessingEvent(Instant.now());

        event.setAttempts(2);

        event = repository.saveAndFlush(event);

        Instant staleTimestamp =
                Instant.now().minus(Duration.ofMinutes(10));

        jdbcTemplate.update(
                """
                UPDATE outbox_event
                SET updated_at = ?
                WHERE id = ?
                """,
                Timestamp.from(staleTimestamp),
                event.getId()
        );

        // When
        int recovered =
                recoveryService.recoverStaleEvents(
                        Duration.ofMinutes(5)
                );

        // Then
        assertThat(recovered).isEqualTo(1);

        OutboxEventEntity persisted =
                repository.findById(event.getId()).orElseThrow();

        assertThat(persisted.getStatus())
                .isEqualTo(OutboxEventStatusType.PENDING);

        assertThat(persisted.getAttempts())
                .isEqualTo(2);
    }

    @Test
    void shouldNotRecoverFreshProcessingEvent() {

        OutboxEventEntity event =
                createProcessingEvent(Instant.now());

        repository.save(event);

        int recovered =
                recoveryService.recoverStaleEvents(
                        Duration.ofMinutes(5)
                );

        assertThat(recovered).isZero();

        OutboxEventEntity persisted =
                repository.findById(event.getId()).orElseThrow();

        assertThat(persisted.getStatus())
                .isEqualTo(OutboxEventStatusType.PROCESSING);
    }

    private OutboxEventEntity createProcessingEvent(Instant updatedAt) {
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
                .status(OutboxEventStatusType.PROCESSING)
                .createdAt(Instant.now())
                .publishedAt(null)
                .attempts(1)
                .lastError(null)
                .updatedAt(updatedAt)
                .build();
    }
}