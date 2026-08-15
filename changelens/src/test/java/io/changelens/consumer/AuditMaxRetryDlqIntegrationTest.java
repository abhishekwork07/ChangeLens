package io.changelens.consumer;

import io.changelens.core.context.AuditContext;
import io.changelens.core.domain.actor.Actor;
import io.changelens.core.domain.actor.ActorType;
import io.changelens.core.domain.audit.AuditEvent;
import io.changelens.core.domain.audit.AuditEventType;
import io.changelens.core.domain.change.ChangeSet;
import io.changelens.core.domain.resource.Resource;
import io.changelens.core.enums.AuditStatusType;
import io.changelens.core.enums.DlqStatusType;
import io.changelens.processing.ProcessingClaimResult;
import io.changelens.processing.dlq.AuditDlqService;
import io.changelens.processing.idempotency.IdempotencyService;
import io.changelens.storage.entity.AuditDlqEntity;
import io.changelens.storage.entity.AuditEventEntity;
import io.changelens.storage.entity.AuditProcessingEntity;
import io.changelens.storage.mapper.AuditEventMapper;
import io.changelens.storage.repository.AuditDlqRepository;
import io.changelens.storage.repository.AuditEventRepository;
import io.changelens.storage.repository.AuditProcessingRepository;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
class AuditMaxRetryDlqIntegrationTest {

    @TestConfiguration
    static class TestKafkaConfig {

        @Bean
        @Primary
        ConsumerFactory<String, String> testConsumerFactory(
                KafkaProperties kafkaProperties) {

            Map<String, Object> props =
                    kafkaProperties.buildConsumerProperties();

            return new DefaultKafkaConsumerFactory<>(
                    props,
                    new StringDeserializer(),
                    new StringDeserializer()
            );
        }
    }

    private static final int MAX_ATTEMPTS = 3;

    private static final String ERROR_MESSAGE =
            "Maximum processing attempts exceeded";

    @Autowired
    private IdempotencyService idempotencyService;

    @Autowired
    private AuditDlqService auditDlqService;

    @Autowired
    private AuditProcessingRepository auditProcessingRepository;

    @Autowired
    private AuditEventRepository auditEventRepository;

    @Autowired
    private AuditDlqRepository auditDlqRepository;

    @Autowired
    private AuditEventMapper auditEventMapper;

    @Autowired
    private PlatformTransactionManager transactionManager;

    @BeforeEach
    void setUp() {

        TransactionTemplate transactionTemplate =
                new TransactionTemplate(transactionManager);

        transactionTemplate.executeWithoutResult(status -> {
            auditDlqRepository.deleteAll();
            auditProcessingRepository.deleteAll();
            auditEventRepository.deleteAll();
        });
    }

    @Test
    void shouldMoveEventToDlqWhenMaximumAttemptsAreExceeded() {

        UUID eventId = UUID.randomUUID();

        String payload =
                createAuditEventPayload(eventId);

        createMaxAttemptProcessingRecord(eventId);

        /*
         * The event has already reached maxAttempts.
         *
         * Therefore tryClaim() must not start another attempt.
         */
        ProcessingClaimResult result =
                idempotencyService.tryClaim(eventId);

        assertThat(result)
                .isEqualTo(
                        ProcessingClaimResult.MAX_ATTEMPTS_REACHED
                );

        /*
         * Load the processing record that caused the
         * maximum-attempt condition.
         */
        AuditProcessingEntity processing =
                auditProcessingRepository
                        .findById(eventId)
                        .orElseThrow();

        assertThat(processing.getStatus())
                .isEqualTo(AuditStatusType.PROCESSING);

        assertThat(processing.getAttempts())
                .isEqualTo(MAX_ATTEMPTS);

        /*
         * Simulate the processor routing the event to DLQ.
         */
        AuditEvent event =
                createAuditEvent(eventId);

        auditDlqService.moveToDlq(
                event.eventId(),
                payload,
                ERROR_MESSAGE
        );

        /*
         * Verify audit_processing was marked FAILED.
         */
        AuditProcessingEntity failedProcessing =
                auditProcessingRepository
                        .findById(eventId)
                        .orElseThrow();

        assertThat(failedProcessing.getStatus())
                .isEqualTo(AuditStatusType.FAILED);

        assertThat(failedProcessing.getAttempts())
                .isEqualTo(MAX_ATTEMPTS);

        assertThat(failedProcessing.getLastError())
                .isEqualTo(ERROR_MESSAGE);

        /*
         * Verify exactly one DLQ record was created.
         */
        List<AuditDlqEntity> dlqEntries =
                auditDlqRepository.findAll();

        assertThat(dlqEntries)
                .hasSize(1);

        AuditDlqEntity dlq =
                dlqEntries.get(0);

        assertThat(dlq.getEventId())
                .isEqualTo(eventId);

        assertThat(dlq.getStatus())
                .isEqualTo(DlqStatusType.FAILED);

        assertThat(dlq.getAttempts())
                .isEqualTo(MAX_ATTEMPTS);

        assertThat(dlq.getPayload())
                .isEqualTo(payload);

        assertThat(dlq.getErrorMessage())
                .isEqualTo(ERROR_MESSAGE);

        assertThat(dlq.getFailedAt())
                .isNotNull();

        assertThat(dlq.getResolvedAt())
                .isNull();
    }

    private void createMaxAttemptProcessingRecord(
            UUID eventId) {

        TransactionTemplate transactionTemplate =
                new TransactionTemplate(transactionManager);

        transactionTemplate.executeWithoutResult(status -> {

            createAuditEventEntity(eventId);

            Instant now = Instant.now();

            Instant staleTime =
                    Instant.now().minus(Duration.ofHours(1));

            AuditProcessingEntity processing =
                    AuditProcessingEntity.builder()
                            .eventId(eventId)
                            .status(AuditStatusType.PROCESSING)
                            .attempts(MAX_ATTEMPTS)
                            .receivedAt(staleTime)
                            .processedAt(null)
                            .lastError(null)
                            .createdAt(staleTime)
                            .updatedAt(staleTime)
                            .build();

            auditProcessingRepository
                    .saveAndFlush(processing);

            auditProcessingRepository.updateUpdatedAt(
                    eventId,
                    staleTime
            );

            auditProcessingRepository
                    .saveAndFlush(processing);
        });
    }

    private void createAuditEventEntity(UUID eventId) {

        AuditEvent event =
                createAuditEvent(eventId);

        AuditEventEntity entity =
                auditEventMapper.toEntity(event);

        auditEventRepository
                .saveAndFlush(entity);
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

    private String createAuditEventPayload(UUID eventId) {

        /*
         * The exact JSON serialization isn't important for this
         * service-level test; the important requirement is that
         * the original payload is preserved in audit_dlq.
         */
        return "{\"eventId\":\""
                + eventId
                + "\",\"eventType\":\"CREATE\"}";
    }
}