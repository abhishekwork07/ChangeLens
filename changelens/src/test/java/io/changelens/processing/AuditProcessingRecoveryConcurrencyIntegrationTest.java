package io.changelens.processing;

import io.changelens.core.context.AuditContext;
import io.changelens.core.domain.actor.Actor;
import io.changelens.core.domain.actor.ActorType;
import io.changelens.core.domain.audit.AuditEvent;
import io.changelens.core.domain.audit.AuditEventType;
import io.changelens.core.domain.change.ChangeSet;
import io.changelens.core.domain.resource.Resource;
import io.changelens.core.enums.AuditStatusType;
import io.changelens.storage.entity.AuditEventEntity;
import io.changelens.storage.entity.AuditProcessingEntity;
import io.changelens.storage.mapper.AuditEventMapper;
import io.changelens.storage.repository.AuditEventRepository;
import io.changelens.storage.repository.AuditProcessingRepository;
import io.changelens.support.IntegrationTestContainers;
import jakarta.persistence.EntityManager;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
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
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Import(IntegrationTestContainers.class)
class AuditProcessingRecoveryConcurrencyIntegrationTest {

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
    private AuditProcessingRepository auditProcessingRepository;

    @Autowired
    private AuditEventRepository auditEventRepository;

    @Autowired
    private AuditEventMapper auditEventMapper;

    @Autowired
    private EntityManager entityManager;

    @Autowired
    private PlatformTransactionManager transactionManager;

    @BeforeEach
    void setUp() {

        TransactionTemplate transactionTemplate =
                new TransactionTemplate(transactionManager);

        transactionTemplate.executeWithoutResult(status -> {
            auditProcessingRepository.deleteAll();
            auditEventRepository.deleteAll();
        });
    }

    @Test
    void shouldAllowOnlyOneConcurrentStaleRecovery()
            throws Exception {

        UUID eventId = UUID.randomUUID();

        /*
         * Create the complete stale state in a transaction
         * that is committed before the concurrent workers start.
         */
        Instant staleTime =
                createStaleProcessingRecord(eventId);

        entityManager.clear();

        /*
         * Verify that the initial state is really committed.
         */
        verifyInitialState(eventId);

        /*
         * Both workers will execute the repository UPDATE
         * inside their own independent transactions.
         */
        ExecutorService executor =
                Executors.newFixedThreadPool(2);

        try {

            CountDownLatch start =
                    new CountDownLatch(1);

            Callable<Integer> reclaim =
                    () -> {

                        start.await(
                                10,
                                TimeUnit.SECONDS
                        );

                        TransactionTemplate transactionTemplate =
                                new TransactionTemplate(
                                        transactionManager
                                );

                        return transactionTemplate.execute(
                                status ->
                                        auditProcessingRepository
                                                .reclaimStaleProcessing(
                                                        eventId,
                                                        Instant.now()
                                                                .minus(Duration.ofMinutes(30)),
                                                        Instant.now(),
                                                        3
                                                )
                        );
                    };

            Future<Integer> first =
                    executor.submit(reclaim);

            Future<Integer> second =
                    executor.submit(reclaim);

            /*
             * Release both workers at approximately the
             * same time.
             */
            start.countDown();

            int updated1 =
                    first.get(
                            10,
                            TimeUnit.SECONDS
                    );

            int updated2 =
                    second.get(
                            10,
                            TimeUnit.SECONDS
                    );

            /*
             * Exactly one UPDATE must succeed.
             *
             * Expected:
             *
             * Thread A -> 1
             * Thread B -> 0
             *
             * or vice versa.
             */
            assertThat(updated1 + updated2)
                    .isEqualTo(1);

            /*
             * Reload from the database because the native
             * UPDATE bypasses Hibernate's first-level cache.
             */
            entityManager.clear();

            AuditProcessingEntity updated =
                    auditProcessingRepository
                            .findById(eventId)
                            .orElseThrow();

            assertThat(updated.getStatus())
                    .isEqualTo(AuditStatusType.PROCESSING);

            /*
             * Initial:
             *
             * attempts = 1
             *
             * Exactly one stale recovery:
             *
             * attempts = 2
             */
            assertThat(updated.getAttempts())
                    .isEqualTo(2);

            assertThat(updated.getUpdatedAt())
                    .isAfter(staleTime);

        } finally {

            executor.shutdownNow();

            if (!executor.awaitTermination(
                    5,
                    TimeUnit.SECONDS)) {

                executor.shutdownNow();
            }
        }
    }

    /**
     * Creates:
     *
     * audit_event
     *      |
     *      | FK
     *      v
     * audit_processing
     *
     * with a stale PROCESSING state.
     *
     * The transaction is committed before this method returns.
     */
    private Instant createStaleProcessingRecord(
            UUID eventId) {

        TransactionTemplate transactionTemplate =
                new TransactionTemplate(transactionManager);

        Instant staleTime =
                Instant.now().minus(Duration.ofHours(1));

        transactionTemplate.executeWithoutResult(status -> {

            createAuditEvent(eventId);

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

            auditProcessingRepository
                    .saveAndFlush(processing);

            /*
             * @LastModifiedDate can modify updatedAt.
             * Force the persisted value to be stale.
             */
            auditProcessingRepository.updateUpdatedAt(
                    eventId,
                    staleTime
            );
        });

        return staleTime;
    }

    private void verifyInitialState(UUID eventId) {

        AuditEventEntity event =
                auditEventRepository
                        .findById(eventId)
                        .orElseThrow(() ->
                                new AssertionError(
                                        "audit_event was not created"
                                ));

        assertThat(event.getEventId())
                .isEqualTo(eventId);

        AuditProcessingEntity processing =
                auditProcessingRepository
                        .findById(eventId)
                        .orElseThrow(() ->
                                new AssertionError(
                                        "audit_processing was not created"
                                ));

        assertThat(processing.getStatus())
                .isEqualTo(AuditStatusType.PROCESSING);

        assertThat(processing.getAttempts())
                .isEqualTo(1);

        assertThat(processing.getUpdatedAt())
                .isBefore(
                        Instant.now()
                                .minus(Duration.ofMinutes(30))
                );
    }

    private void createAuditEvent(UUID eventId) {

        AuditEvent event =
                new AuditEvent(
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

        auditEventRepository
                .saveAndFlush(entity);
    }
}