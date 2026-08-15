package io.changelens.processing;

import io.changelens.core.context.AuditContext;
import io.changelens.core.domain.actor.Actor;
import io.changelens.core.domain.actor.ActorType;
import io.changelens.core.domain.audit.AuditEvent;
import io.changelens.core.domain.audit.AuditEventType;
import io.changelens.core.domain.change.ChangeSet;
import io.changelens.core.domain.resource.Resource;
import io.changelens.core.enums.AuditStatusType;
import io.changelens.processing.idempotency.IdempotencyService;
import io.changelens.storage.entity.AuditEventEntity;
import io.changelens.storage.entity.AuditProcessingEntity;
import io.changelens.storage.mapper.AuditEventMapper;
import io.changelens.storage.repository.AuditEventRepository;
import io.changelens.storage.repository.AuditProcessingRepository;
import jakarta.persistence.EntityManager;
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
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
class AuditProcessingRecoveryIntegrationTest {

    @Autowired
    private EntityManager entityManager;

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

    @Autowired
    private IdempotencyService idempotencyService;

    @Autowired
    private AuditProcessingRepository auditProcessingRepository;

    @Autowired
    private AuditEventRepository auditEventRepository;

    @Autowired
    private AuditEventMapper auditEventMapper;

    @BeforeEach
    void setUp() {
        auditProcessingRepository.deleteAll();
        auditEventRepository.deleteAll();
    }

    @Test
    @Transactional
    void shouldRetryStaleProcessingEvent() {

        UUID eventId = UUID.randomUUID();

        createAuditEvent(eventId);

        Instant staleTime =
                Instant.now().minus(Duration.ofHours(1));

        AuditProcessingEntity processing =
                AuditProcessingEntity.builder()
                        .eventId(eventId)
                        .status(AuditStatusType.PROCESSING)
                        .attempts(1)
                        .receivedAt(staleTime)
                        .processedAt(null)
                        .lastError(null)
                        .createdAt(staleTime)
                        .updatedAt(staleTime)
                        .build();

        auditProcessingRepository.saveAndFlush(processing);

        auditProcessingRepository.updateUpdatedAt(
                eventId,
                staleTime
        );

        entityManager.clear();

        ProcessingClaimResult result =
                idempotencyService.tryClaim(eventId);

        assertThat(result)
                .isEqualTo(ProcessingClaimResult.RETRY_STARTED);

        entityManager.clear();

        AuditProcessingEntity updated =
                auditProcessingRepository
                        .findById(eventId)
                        .orElseThrow();

        assertThat(updated.getStatus())
                .isEqualTo(AuditStatusType.PROCESSING);

        assertThat(updated.getAttempts())
                .isEqualTo(2);

        assertThat(updated.getUpdatedAt())
                .isAfter(staleTime);
    }

    private void createAuditEvent(UUID eventId) {

        AuditEvent event = new AuditEvent(
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

        AuditEventEntity entity =
                auditEventMapper.toEntity(event);

        auditEventRepository.saveAndFlush(entity);
    }
}