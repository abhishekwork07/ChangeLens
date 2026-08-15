package io.changelens.consumer;

import io.changelens.core.context.AuditContext;
import io.changelens.core.domain.actor.Actor;
import io.changelens.core.domain.actor.ActorType;
import io.changelens.core.domain.audit.AuditEvent;
import io.changelens.core.domain.audit.AuditEventType;
import io.changelens.core.domain.change.ChangeSet;
import io.changelens.core.domain.resource.Resource;
import io.changelens.core.enums.AuditStatusType;
import io.changelens.processing.processor.AuditEventProcessor;
import io.changelens.storage.entity.AuditEventEntity;
import io.changelens.storage.entity.AuditProcessingEntity;
import io.changelens.storage.mapper.AuditEventMapper;
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

import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
class AuditEventProcessingIntegrationTest {

    /**
     * The full application context creates KafkaConsumerConfig,
     * whose listener container factory requires a
     * ConsumerFactory<String, String>.
     *
     * This test does not actually need a Kafka broker, but the
     * application context still needs the Kafka infrastructure
     * beans to be created successfully.
     */
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
    private AuditEventProcessor processor;

    @Autowired
    private AuditProcessingRepository auditProcessingRepository;

    @Autowired
    private AuditEventRepository auditEventRepository;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private AuditEventMapper auditEventMapper;

    @BeforeEach
    void setUp() {

        /*
         * audit_processing has a foreign key to audit_event,
         * therefore delete the child table first.
         */
        auditProcessingRepository.deleteAll();
        auditEventRepository.deleteAll();
    }

    @Test
    void shouldProcessValidAuditEventSuccessfully()
            throws Exception {

        UUID eventId = UUID.randomUUID();

        AuditEvent event =
                createAuditEvent(eventId);

        /*
         * The processing record references audit_event,
         * therefore the parent event must exist first.
         */
        AuditEventEntity entity =
                auditEventMapper.toEntity(event);

        auditEventRepository.saveAndFlush(entity);

        String payload =
                objectMapper.writeValueAsString(event);

        processor.process(payload);

        AuditProcessingEntity processing =
                auditProcessingRepository
                        .findById(eventId)
                        .orElseThrow();

        assertThat(processing.getStatus())
                .isEqualTo(AuditStatusType.PROCESSED);

        assertThat(processing.getAttempts())
                .isEqualTo(1);

        assertThat(processing.getProcessedAt())
                .isNotNull();

        assertThat(processing.getLastError())
                .isNull();
    }

    @Test
    void shouldIgnoreDuplicateAuditEvent()
            throws Exception {

        UUID eventId = UUID.randomUUID();

        AuditEvent event =
                createAuditEvent(eventId);

        AuditEventEntity entity =
                auditEventMapper.toEntity(event);

        auditEventRepository.saveAndFlush(entity);

        String payload =
                objectMapper.writeValueAsString(event);

        /*
         * First delivery.
         */
        processor.process(payload);

        AuditProcessingEntity firstProcessing =
                auditProcessingRepository
                        .findById(eventId)
                        .orElseThrow();

        assertThat(firstProcessing.getStatus())
                .isEqualTo(AuditStatusType.PROCESSED);

        assertThat(firstProcessing.getAttempts())
                .isEqualTo(1);

        Instant firstProcessedAt =
                firstProcessing.getProcessedAt();

        /*
         * Duplicate delivery.
         */
        processor.process(payload);

        AuditProcessingEntity secondProcessing =
                auditProcessingRepository
                        .findById(eventId)
                        .orElseThrow();

        assertThat(secondProcessing.getStatus())
                .isEqualTo(AuditStatusType.PROCESSED);

        /*
         * Duplicate must not create another attempt.
         */
        assertThat(secondProcessing.getAttempts())
                .isEqualTo(1);

        /*
         * Duplicate must not change the original
         * processing completion timestamp.
         */
        assertThat(secondProcessing.getProcessedAt())
                .isEqualTo(firstProcessedAt);

        assertThat(secondProcessing.getLastError())
                .isNull();
    }

    @Test
    void shouldProcessSameAuditEventOnlyOnceWhenDeliveredConcurrently()
            throws Exception {

        UUID eventId = UUID.randomUUID();

        AuditEvent event =
                createAuditEvent(eventId);

        AuditEventEntity entity =
                auditEventMapper.toEntity(event);

        auditEventRepository.saveAndFlush(entity);

        String payload =
                objectMapper.writeValueAsString(event);

        ExecutorService executor =
                Executors.newFixedThreadPool(2);

        CountDownLatch startLatch =
                new CountDownLatch(1);

        try {

            Callable<Void> task = () -> {

                startLatch.await();

                processor.process(payload);

                return null;
            };

            Future<Void> first =
                    executor.submit(task);

            Future<Void> second =
                    executor.submit(task);

            /*
             * Release both consumers at approximately
             * the same time.
             */
            startLatch.countDown();

            first.get();
            second.get();

        } finally {

            executor.shutdownNow();
        }

        AuditProcessingEntity processing =
                auditProcessingRepository
                        .findById(eventId)
                        .orElseThrow();

        assertThat(processing.getStatus())
                .isEqualTo(AuditStatusType.PROCESSED);

        /*
         * Only one consumer must successfully claim
         * the event.
         */
        assertThat(processing.getAttempts())
                .isEqualTo(1);

        assertThat(processing.getProcessedAt())
                .isNotNull();

        assertThat(processing.getLastError())
                .isNull();
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