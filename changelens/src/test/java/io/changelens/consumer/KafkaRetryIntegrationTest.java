//package io.changelens.consumer;
//
//import io.changelens.core.context.AuditContext;
//import io.changelens.core.domain.actor.Actor;
//import io.changelens.core.domain.actor.ActorType;
//import io.changelens.core.domain.audit.AuditEvent;
//import io.changelens.core.domain.audit.AuditEventType;
//import io.changelens.core.domain.change.ChangeSet;
//import io.changelens.core.domain.resource.Resource;
//import io.changelens.core.enums.AuditStatusType;
//import io.changelens.core.enums.OutboxEventStatusType;
//import io.changelens.outbox.entity.OutboxEventEntity;
//import io.changelens.outbox.publisher.OutboxPublisher;
//import io.changelens.outbox.repository.OutboxEventRepository;
//import io.changelens.processing.service.AuditProcessingException;
//import io.changelens.processing.service.AuditProcessingService;
//import io.changelens.storage.entity.AuditEventEntity;
//import io.changelens.storage.entity.AuditProcessingEntity;
//import io.changelens.storage.mapper.AuditEventMapper;
//import io.changelens.storage.repository.AuditEventRepository;
//import io.changelens.storage.repository.AuditProcessingRepository;
//
//import io.changelens.support.IntegrationTestContainers;
//import org.apache.kafka.common.serialization.StringDeserializer;
//
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.Test;
//
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
//import org.springframework.boot.test.context.SpringBootTest;
//import org.springframework.boot.test.context.TestConfiguration;
//import org.springframework.context.annotation.Bean;
//import org.springframework.context.annotation.Import;
//import org.springframework.context.annotation.Primary;
//
//import org.springframework.kafka.core.ConsumerFactory;
//import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
//import org.springframework.kafka.test.context.EmbeddedKafka;
//
//import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
//
//import tools.jackson.databind.ObjectMapper;
//
//import java.time.Duration;
//import java.time.Instant;
//import java.util.List;
//import java.util.Map;
//import java.util.UUID;
//import java.util.concurrent.atomic.AtomicInteger;
//
//import static org.assertj.core.api.Assertions.assertThat;
//import static org.mockito.ArgumentMatchers.any;
//import static org.mockito.Mockito.doAnswer;
//import static org.awaitility.Awaitility.await;
//
//
//@SpringBootTest(properties = {
//        "changelens.kafka.topics.audit-events=audit-events",
//        "changelens.kafka.consumer.group-id=changelens-kafka-retry-test"
//})
//@EmbeddedKafka(
//        partitions = 1,
//        topics = "audit-events"
//)
//@Import(IntegrationTestContainers.class)
//class KafkaRetryIntegrationTest {
//
//    private static final String TOPIC = "audit-events";
//
//    /**
//     * The production KafkaConsumerConfig requires a
//     * ConsumerFactory<String, String>.
//     *
//     * EmbeddedKafka supplies the broker, but this test
//     * configuration supplies the ConsumerFactory required
//     * by the application's KafkaListenerContainerFactory.
//     */
//    @TestConfiguration
//    static class TestKafkaConsumerConfig {
//
//        @Bean
//        @Primary
//        ConsumerFactory<String, String> testConsumerFactory(
//                KafkaProperties kafkaProperties) {
//
//            Map<String, Object> props =
//                    kafkaProperties.buildConsumerProperties();
//
//            return new DefaultKafkaConsumerFactory<>(
//                    props,
//                    new StringDeserializer(),
//                    new StringDeserializer()
//            );
//        }
//    }
//
//    @Autowired
//    private AuditProcessingRepository auditProcessingRepository;
//
//    @Autowired
//    private AuditEventRepository auditEventRepository;
//
//    @Autowired
//    private OutboxPublisher outboxPublisher;
//
//    @Autowired
//    private OutboxEventRepository outboxEventRepository;
//
//    @Autowired
//    private AuditEventMapper auditEventMapper;
//
//    @Autowired
//    private ObjectMapper objectMapper;
//
//    @MockitoSpyBean
//    private AuditProcessingService processingService;
//
//
//    @BeforeEach
//    void setUp() {
//
//        /*
//         * Clean dependent tables first because
//         * audit_processing and outbox records may
//         * reference audit_event.
//         */
//        auditProcessingRepository.deleteAll();
//
//        outboxEventRepository.deleteAll();
//
//        auditEventRepository.deleteAll();
//    }
//
//
//    @Test
//    void shouldRetryWhenAuditProcessingFails()
//            throws Exception {
//
//        UUID eventId = UUID.randomUUID();
//
//        AuditEvent event =
//                createAuditEvent(eventId);
//
//
//        /*
//         * audit_processing.event_id has a foreign key
//         * relationship with audit_event.event_id.
//         *
//         * Therefore the parent audit event must exist
//         * before Kafka processing starts.
//         */
//        AuditEventEntity auditEventEntity =
//                auditEventMapper.toEntity(event);
//
//        auditEventRepository.saveAndFlush(
//                auditEventEntity
//        );
//
//
//        /*
//         * Create the actual outbox event.
//         *
//         * OutboxPublisher will publish the payload
//         * through the production Kafka producer.
//         */
//        OutboxEventEntity outboxEvent =
//                createOutboxEvent(event);
//
//        outboxEventRepository.saveAndFlush(
//                outboxEvent
//        );
//
//
//        AtomicInteger processingAttempts =
//                new AtomicInteger();
//
//
//        /*
//         * First invocation intentionally fails.
//         *
//         * The real AuditEventProcessor catches the
//         * exception, marks audit_processing as FAILED,
//         * and rethrows the exception.
//         *
//         * Kafka's DefaultErrorHandler then redelivers
//         * the same Kafka record.
//         *
//         * On the second invocation the real processing
//         * service executes.
//         */
//        doAnswer(invocation -> {
//
//            int attempt =
//                    processingAttempts.incrementAndGet();
//
//            if (attempt == 1) {
//
//                throw new AuditProcessingException(
//                        "Simulated processing failure"
//                );
//            }
//
//            return invocation.callRealMethod();
//
//        }).when(processingService)
//                .process(any(AuditEvent.class));
//
//
//        /*
//         * Publish using the actual OutboxPublisher.
//         *
//         * This means the test exercises:
//         *
//         * OutboxPublisher
//         *       ↓
//         * KafkaEventProducer
//         *       ↓
//         * Embedded Kafka
//         *       ↓
//         * AuditEventConsumer
//         *       ↓
//         * AuditEventProcessor
//         *       ↓
//         * AuditProcessingService
//         */
//        outboxPublisher.publish(1);
//
//
//        /*
//         * First wait until Kafka has delivered the
//         * message and audit_processing has been created.
//         */
//        await()
//                .atMost(Duration.ofSeconds(15))
//                .pollInterval(Duration.ofMillis(100))
//                .untilAsserted(() -> {
//
//                    AuditProcessingEntity processing =
//                            auditProcessingRepository
//                                    .findById(eventId)
//                                    .orElseThrow();
//
//                    assertThat(processing.getAttempts())
//                            .isGreaterThanOrEqualTo(1);
//                });
//
//
//        /*
//         * Wait for:
//         *
//         * 1. First processing attempt
//         * 2. FAILED state
//         * 3. Kafka retry
//         * 4. RETRY_STARTED
//         * 5. Second processing attempt
//         * 6. PROCESSED state
//         */
//        await()
//                .atMost(Duration.ofSeconds(20))
//                .pollInterval(Duration.ofMillis(100))
//                .untilAsserted(() -> {
//
//                    AuditProcessingEntity processing =
//                            auditProcessingRepository
//                                    .findById(eventId)
//                                    .orElseThrow();
//
//                    assertThat(processing.getStatus())
//                            .isEqualTo(
//                                    AuditStatusType.PROCESSED
//                            );
//
//                    assertThat(processing.getAttempts())
//                            .isEqualTo(2);
//
//                    assertThat(processing.getProcessedAt())
//                            .isNotNull();
//
//                    assertThat(processing.getLastError())
//                            .isNull();
//                });
//
//
//        /*
//         * Exactly two actual service invocations:
//         *
//         * attempt 1 -> failure
//         * attempt 2 -> success
//         */
//        assertThat(processingAttempts.get())
//                .isEqualTo(2);
//    }
//
//
//    private OutboxEventEntity createOutboxEvent(
//            AuditEvent event)
//            throws Exception {
//
//        String json =
//                objectMapper.writeValueAsString(event);
//
//        Map<String, Object> payload =
//                objectMapper.readValue(
//                        json,
//                        Map.class
//                );
//
//        Instant now =
//                Instant.now();
//
//        return OutboxEventEntity.builder()
//                .eventId(event.eventId())
//                .aggregateType("AUDIT_EVENT")
//                .aggregateId(
//                        event.eventId().toString()
//                )
//                .eventType(event.eventType())
//                .payload(payload)
//                .status(
//                        OutboxEventStatusType.PENDING
//                )
//                .createdAt(now)
//                .publishedAt(null)
//                .attempts(0)
//                .lastError(null)
//                .updatedAt(now)
//                .build();
//    }
//
//
//    private AuditEvent createAuditEvent(
//            UUID eventId) {
//
//        return new AuditEvent(
//                eventId,
//                1,
//                "tenant-1",
//                AuditEventType.CREATE,
//                Instant.now(),
//                "CREATE",
//                new Actor(
//                        ActorType.USER,
//                        "user-1",
//                        "Test User"
//                ),
//                new Resource(
//                        "RESOURCE",
//                        "resource-1",
//                        "Test Resource"
//                ),
//                new ChangeSet(
//                        "test summary",
//                        List.of()
//                ),
//                null,
//                null,
//                new AuditContext(
//                        "ChangeLens",
//                        "1.0.0",
//                        "audit-service",
//                        "test",
//                        "request-1",
//                        "correlation-1",
//                        "trace-1",
//                        "127.0.0.1",
//                        "JUnit"
//                ),
//                Map.of()
//        );
//    }
//}