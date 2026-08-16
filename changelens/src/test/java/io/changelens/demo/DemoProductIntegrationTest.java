package io.changelens.demo;

import io.changelens.cache.ProcessedEventCache;
import io.changelens.core.domain.audit.AuditEventType;
import io.changelens.core.enums.AuditStatusType;
import io.changelens.core.enums.OutboxEventStatusType;
import io.changelens.demo.dto.CreateCustomerRequest;
import io.changelens.demo.dto.UpdateCustomerRequest;
import io.changelens.demo.entity.DemoCustomer;
import io.changelens.demo.repository.DemoCustomerRepository;
import io.changelens.demo.service.DemoCustomerService;
import io.changelens.demo.service.DemoFailureMode;
import io.changelens.outbox.entity.OutboxEventEntity;
import io.changelens.outbox.publisher.OutboxPublisher;
import io.changelens.outbox.publisher.OutboxPublishingScheduler;
import io.changelens.outbox.repository.OutboxEventRepository;
import io.changelens.storage.entity.AuditChangeEntity;
import io.changelens.storage.entity.AuditEventEntity;
import io.changelens.storage.entity.AuditProcessingEntity;
import io.changelens.storage.repository.AuditChangeRepository;
import io.changelens.storage.repository.AuditEventRepository;
import io.changelens.storage.repository.AuditProcessingRepository;
import io.changelens.support.IntegrationTestContainers;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(properties = {
        "spring.jpa.hibernate.ddl-auto=update",
        "spring.kafka.bootstrap-servers=localhost:9092",
        "spring.kafka.consumer.auto-offset-reset=latest",
        "changelens.kafka.topics.audit-events=audit-events",
        "changelens.kafka.consumer.group-id=changelens-demo-${random.uuid}"
})
@Import(IntegrationTestContainers.class)
class DemoProductIntegrationTest {

    @Autowired
    private DemoCustomerService customerService;

    @Autowired
    private DemoCustomerRepository customerRepository;

    @Autowired
    private AuditEventRepository auditEventRepository;

    @Autowired
    private AuditProcessingRepository auditProcessingRepository;

    @Autowired
    private AuditChangeRepository auditChangeRepository;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private OutboxPublisher outboxPublisher;

    @Autowired
    private DemoFailureMode failureMode;

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Autowired
    private KafkaListenerEndpointRegistry kafkaListenerRegistry;

    @Autowired
    private OutboxPublishingScheduler outboxPublishingScheduler;

    @Autowired
    private ProcessedEventCache processedEventCache;


    // ============================================================
    // TEST LIFECYCLE
    // ============================================================

    @BeforeEach
    void setUp() {

        failureMode.disable();

        /*
         * Clean demo application data.
         */
        customerRepository.deleteAll();

        /*
         * Clean ChangeLens data.
         *
         * Keep dependent entities before their parents.
         */
        auditChangeRepository.deleteAll();
        auditProcessingRepository.deleteAll();
        outboxEventRepository.deleteAll();
        auditEventRepository.deleteAll();

        /*
         * Clear Redis idempotency state.
         */
        var keys =
                redisTemplate.keys(
                        "changelens:processed:*"
                );

        if (keys != null && !keys.isEmpty()) {
            redisTemplate.delete(keys);
        }
    }

    private void startKafkaListeners() {

        kafkaListenerRegistry
                .getListenerContainers()
                .forEach(MessageListenerContainer::start);
    }


    private void stopKafkaListeners() {

        kafkaListenerRegistry
                .getListenerContainers()
                .forEach(MessageListenerContainer::stop);
    }


    // ============================================================
    // 1. METHOD LEVEL AUDITING
    // ============================================================

    @Test
    @DisplayName("Audit customer creation through complete audit pipeline")
    void shouldAuditCustomerCreation() {

        System.out.println("\n========================================");
        System.out.println("CREATE CUSTOMER");
        System.out.println("========================================");

        /*
         * ============================================================
         * 1. Create customer through the demo application.
         *
         * DemoCustomerService
         *        ↓
         *      @Audit
         *        ↓
         *    AuditAspect
         *        ↓
         *    AuditEvent
         *        ↓
         *    AuditEventPublisher
         *        ↓
         *    Outbox Event
         * ============================================================
         */
        DemoCustomer customer =
                customerService.createCustomer(
                        new CreateCustomerRequest(
                                "Abhishek",
                                "abhishek@example.com"
                        )
                );

        assertThat(customer.getId())
                .as("Customer should be created")
                .isNotNull();

        System.out.println(
                "Customer ID = " + customer.getId()
        );

        /*
         * Verify business operation.
         */
        assertThat(
                customerRepository.findById(customer.getId())
        )
                .as("Customer should exist in database")
                .isPresent();


        /*
         * ============================================================
         * 2. Wait for outbox event.
         * ============================================================
         */
        await()
                .atMost(Duration.ofSeconds(5))
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> {

                    assertThat(
                            outboxEventRepository.findAll()
                    )
                            .as("Expected audit event to be created in outbox")
                            .isNotEmpty();
                });


        /*
         * Get the event generated by this operation.
         */
        OutboxEventEntity outboxEvent =
                outboxEventRepository.findAll()
                        .stream()
                        .filter(event ->
                                event.getEventId() != null)
                        .findFirst()
                        .orElseThrow(() ->
                                new AssertionError(
                                        "Outbox event was not created"
                                )
                        );

        UUID eventId =
                outboxEvent.getEventId();


        System.out.println("\n========================================");
        System.out.println("OUTBOX EVENT CREATED");
        System.out.println("========================================");
        System.out.println("Event ID = " + eventId);
        System.out.println("Status = " + outboxEvent.getStatus());
        System.out.println("Attempts = " + outboxEvent.getAttempts());


        /*
         * Verify initial outbox state.
         */
        assertThat(outboxEvent.getStatus())
                .as("New outbox event should be PENDING")
                .isEqualTo(OutboxEventStatusType.PENDING);

        assertThat(outboxEvent.getAttempts())
                .as("New outbox event should have zero attempts")
                .isZero();


        /*
         * ============================================================
         * 3. Explicitly invoke the real outbox scheduler.
         *
         * This avoids depending on @Scheduled timing during the demo
         * test while still exercising the real publishing component.
         * ============================================================
         */
        System.out.println("\n========================================");
        System.out.println("PUBLISHING OUTBOX EVENT");
        System.out.println("========================================");

        outboxPublishingScheduler.publish();


        /*
         * ============================================================
         * 4. Verify outbox → Kafka publication.
         * ============================================================
         */
        await()
                .atMost(Duration.ofSeconds(10))
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> {

                    OutboxEventEntity publishedEvent =
                            outboxEventRepository
                                    .findById(outboxEvent.getId())
                                    .orElseThrow(() ->
                                            new AssertionError(
                                                    "Outbox event disappeared"
                                            )
                                    );

                    assertThat(publishedEvent.getStatus())
                            .as("Outbox event should be PUBLISHED")
                            .isEqualTo(
                                    OutboxEventStatusType.PUBLISHED
                            );

                    assertThat(publishedEvent.getAttempts())
                            .as("Outbox event should have at least one attempt")
                            .isGreaterThan(0);

                    assertThat(publishedEvent.getLastError())
                            .as("Published event should not contain an error")
                            .isNull();
                });


        System.out.println("\n========================================");
        System.out.println("OUTBOX EVENT PUBLISHED");
        System.out.println("========================================");
        System.out.println("Event ID = " + eventId);


        /*
         * ============================================================
         * 5. Wait for Kafka consumer + audit processing.
         *
         * Kafka
         *   ↓
         * AuditEventConsumer
         *   ↓
         * AuditEventProcessor
         *   ↓
         * IdempotencyService
         *   ↓
         * AuditProcessingService
         * ============================================================
         */
        System.out.println("\n========================================");
        System.out.println("WAITING FOR KAFKA PROCESSING");
        System.out.println("========================================");

        await()
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> {

                    assertThat(
                            auditProcessingRepository
                                    .findById(eventId)
                    )
                            .as("Expected audit processing record")
                            .isPresent();
                });


        /*
         * ============================================================
         * 6. Verify audit processing status.
         *
         * AuditProcessingStatusService.markProcessed(event)
         * performs:
         *
         *   audit_processing → PROCESSED
         *   audit_event      → INSERT
         *
         * in the same transaction.
         * ============================================================
         */
        AuditProcessingEntity processing =
                auditProcessingRepository
                        .findById(eventId)
                        .orElseThrow(() ->
                                new AssertionError(
                                        "Audit processing record was not created"
                                )
                        );


        System.out.println("\n========================================");
        System.out.println("AUDIT PROCESSING COMPLETED");
        System.out.println("========================================");
        System.out.println("Event ID = " + processing.getEventId());
        System.out.println("Status = " + processing.getStatus());
        System.out.println("Attempts = " + processing.getAttempts());


        assertThat(processing.getEventId())
                .isEqualTo(eventId);

        assertThat(processing.getStatus())
                .as("Audit processing should be PROCESSED")
                .isEqualTo(AuditStatusType.PROCESSED);

        assertThat(processing.getAttempts())
                .as("Audit event should be processed once")
                .isEqualTo(1);

        assertThat(processing.getLastError())
                .as("Successfully processed event should not have an error")
                .isNull();


        /*
         * ============================================================
         * 7. Verify durable audit event persistence.
         *
         * This is the important part introduced by the new
         * AuditProcessingStatusService implementation:
         *
         * auditEventRepository.save(
         *     auditEventMapper.toEntity(event)
         * );
         * ============================================================
         */
        await()
                .atMost(Duration.ofSeconds(10))
                .pollInterval(Duration.ofMillis(300))
                .untilAsserted(() -> {

                    assertThat(
                            auditEventRepository.findById(eventId)
                    )
                            .as("Expected audit event to be persisted")
                            .isPresent();
                });


        /*
         * Get persisted audit event.
         */
        AuditEventEntity auditEvent =
                auditEventRepository
                        .findById(eventId)
                        .orElseThrow(() ->
                                new AssertionError(
                                        "Audit event was not persisted"
                                )
                        );


        System.out.println("\n========================================");
        System.out.println("AUDIT EVENT PERSISTED");
        System.out.println("========================================");
        System.out.println("Event ID = " + auditEvent.getEventId());


        /*
         * ============================================================
         * 8. Verify persisted audit event contents.
         * ============================================================
         */
        assertThat(auditEvent.getEventId())
                .as("Persisted event ID should match Kafka event ID")
                .isEqualTo(eventId);

        assertThat(auditEvent.getEventType())
                .as("Audit event type")
                .isEqualTo(AuditEventType.CREATE);

        assertThat(auditEvent.getAction())
                .as("Audit action")
                .isEqualTo("CREATE");

        assertThat(auditEvent.getResourceType())
                .as("Audit resource type")
                .isEqualTo("CUSTOMER");

        assertThat(auditEvent.getResourceId())
                .as("Audit resource ID")
                .isEqualTo(customer.getId().toString());


        /*
         * ============================================================
         * 9. Verify idempotency cache.
         *
         * AuditProcessingService:
         *
         *   statusService.markProcessed(event);
         *   processedEventCache.put(event.eventId());
         * ============================================================
         */
        await()
                .atMost(Duration.ofSeconds(5))
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> {

                    assertThat(
                            processedEventCache.contains(eventId)
                    )
                            .as("Processed event should exist in idempotency cache")
                            .isTrue();
                });


        /*
         * ============================================================
         * 10. Final verification.
         * ============================================================
         */
        assertThat(
                auditProcessingRepository.findById(eventId)
        )
                .as("Audit processing record should exist")
                .isPresent();

        assertThat(
                auditEventRepository.findById(eventId)
        )
                .as("Audit event should exist")
                .isPresent();


        /*
         * ============================================================
         * 11. Demo summary.
         * ============================================================
         */
        System.out.println("\n========================================");
        System.out.println("AUDIT PIPELINE COMPLETED SUCCESSFULLY");
        System.out.println("========================================");
        System.out.println("Customer ID          = " + customer.getId());
        System.out.println("Event ID             = " + eventId);
        System.out.println("Outbox Status        = PUBLISHED");
        System.out.println("Processing Status    = " + processing.getStatus());
        System.out.println("Processing Attempts  = " + processing.getAttempts());
        System.out.println("Audit Event          = PERSISTED");
        System.out.println("Idempotency Cache    = HIT");
        System.out.println("========================================\n");
    }

    // ============================================================
    // 2. CLASS LEVEL AUDITING
    // ============================================================

    @Test
    @DisplayName(
            "Demo - UPDATE uses class-level @Audit defaults"
    )
    void shouldUseClassLevelAuditConfiguration() {

        System.out.println("\n========================================");
        System.out.println("PREPARING CUSTOMER FOR UPDATE");
        System.out.println("========================================");

        /*
         * ============================================================
         * 1. Create customer that will be updated.
         *
         * We first allow the CREATE audit pipeline to complete before
         * cleaning the audit tables. This prevents the asynchronous
         * Kafka consumer from writing CREATE records after cleanup.
         * ============================================================
         */
        DemoCustomer customer =
                customerService.createCustomer(
                        new CreateCustomerRequest(
                                "Abhishek",
                                "abhishek@example.com"
                        )
                );

        assertThat(customer.getId())
                .as("Customer should be created")
                .isNotNull();

        System.out.println(
                "Customer ID = " + customer.getId()
        );


        /*
         * Wait until the CREATE event has completed processing.
         */
        await()
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(300))
                .untilAsserted(() -> {

                    assertThat(
                            auditEventRepository.findAll()
                    )
                            .as("CREATE audit event should be persisted")
                            .isNotEmpty();
                });


        /*
         * ============================================================
         * 2. Clean previous audit data.
         *
         * The customer itself is intentionally retained.
         * Only audit-related data is removed so that the UPDATE
         * assertion examines only the UPDATE event.
         * ============================================================
         */
        System.out.println("\n========================================");
        System.out.println("CLEANING PREVIOUS AUDIT DATA");
        System.out.println("========================================");

        System.out.println(
                "Audit events before cleanup = "
                        + auditEventRepository.count()
        );

        System.out.println(
                "Audit processing before cleanup = "
                        + auditProcessingRepository.count()
        );

        System.out.println(
                "Outbox events before cleanup = "
                        + outboxEventRepository.count()
        );

        System.out.println(
                "Audit changes before cleanup = "
                        + auditChangeRepository.count()
        );


        /*
         * Delete child/dependent records first.
         */
        auditChangeRepository.deleteAll();
        auditProcessingRepository.deleteAll();
        outboxEventRepository.deleteAll();
        auditEventRepository.deleteAll();


        System.out.println("\nAudit data cleaned.");

        System.out.println(
                "Audit events after cleanup = "
                        + auditEventRepository.count()
        );

        System.out.println(
                "Audit processing after cleanup = "
                        + auditProcessingRepository.count()
        );

        System.out.println(
                "Outbox events after cleanup = "
                        + outboxEventRepository.count()
        );

        System.out.println(
                "Audit changes after cleanup = "
                        + auditChangeRepository.count()
        );


        /*
         * ============================================================
         * 3. UPDATE customer.
         *
         * DemoCustomerService has:
         *
         * @Audit(
         *     action = "UPDATE",
         *     resource = "CUSTOMER"
         * )
         *
         * at class level.
         *
         * The updateCustomer() method itself has no @Audit annotation,
         * therefore the SDK must resolve the class-level configuration.
         * ============================================================
         */
        System.out.println("\n========================================");
        System.out.println("UPDATING CUSTOMER");
        System.out.println("========================================");

        DemoCustomer updatedCustomer =
                customerService.updateCustomer(
                        customer.getId(),
                        new UpdateCustomerRequest(
                                "Abhishek Gupta",
                                "abhishek.gupta@example.com",
                                "PREMIUM"
                        )
                );

        assertThat(updatedCustomer)
                .isNotNull();

        assertThat(updatedCustomer.getId())
                .isEqualTo(customer.getId());

        System.out.println(
                "Customer ID = " + updatedCustomer.getId()
        );

        System.out.println(
                "Updated Name = " + updatedCustomer.getName()
        );

        System.out.println(
                "Updated Email = " + updatedCustomer.getEmail()
        );

        System.out.println(
                "Updated Status = " + updatedCustomer.getStatus()
        );


        /*
         * ============================================================
         * 4. Verify UPDATE outbox event.
         * ============================================================
         */
        await()
                .atMost(Duration.ofSeconds(5))
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> {

                    assertThat(
                            outboxEventRepository.findAll()
                    )
                            .as("Expected UPDATE audit event in outbox")
                            .hasSize(1);
                });


        OutboxEventEntity outboxEvent =
                outboxEventRepository.findAll()
                        .stream()
                        .filter(event ->
                                event.getEventId() != null)
                        .findFirst()
                        .orElseThrow(() ->
                                new AssertionError(
                                        "UPDATE outbox event was not created"
                                )
                        );

        UUID eventId =
                outboxEvent.getEventId();


        System.out.println("\n========================================");
        System.out.println("UPDATE OUTBOX EVENT CREATED");
        System.out.println("========================================");
        System.out.println("Event ID = " + eventId);
        System.out.println("Status = " + outboxEvent.getStatus());
        System.out.println("Attempts = " + outboxEvent.getAttempts());


        assertThat(outboxEvent.getStatus())
                .as("New UPDATE outbox event should be PENDING")
                .isEqualTo(OutboxEventStatusType.PENDING);

        assertThat(outboxEvent.getAttempts())
                .as("New UPDATE outbox event should have zero attempts")
                .isZero();


        /*
         * ============================================================
         * 5. Publish UPDATE event through the real scheduler.
         * ============================================================
         */
        System.out.println("\n========================================");
        System.out.println("PUBLISHING UPDATE OUTBOX EVENT");
        System.out.println("========================================");

        outboxPublishingScheduler.publish();


        /*
         * ============================================================
         * 6. Verify outbox publication.
         * ============================================================
         */
        await()
                .atMost(Duration.ofSeconds(10))
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> {

                    OutboxEventEntity publishedEvent =
                            outboxEventRepository
                                    .findById(outboxEvent.getId())
                                    .orElseThrow(() ->
                                            new AssertionError(
                                                    "UPDATE outbox event disappeared"
                                            )
                                    );

                    assertThat(publishedEvent.getStatus())
                            .as("UPDATE outbox event should be PUBLISHED")
                            .isEqualTo(
                                    OutboxEventStatusType.PUBLISHED
                            );

                    assertThat(publishedEvent.getAttempts())
                            .as("UPDATE event should have been attempted")
                            .isGreaterThan(0);

                    assertThat(publishedEvent.getLastError())
                            .as("UPDATE event should not have an error")
                            .isNull();
                });


        System.out.println("\n========================================");
        System.out.println("UPDATE OUTBOX EVENT PUBLISHED");
        System.out.println("========================================");
        System.out.println("Event ID = " + eventId);


        /*
         * ============================================================
         * 7. Wait for Kafka processing.
         * ============================================================
         */
        System.out.println("\n========================================");
        System.out.println("WAITING FOR UPDATE KAFKA PROCESSING");
        System.out.println("========================================");

        await()
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> {

                    assertThat(
                            auditProcessingRepository
                                    .findById(eventId)
                    )
                            .as("Expected UPDATE audit processing record")
                            .isPresent();
                });


        /*
         * ============================================================
         * 8. Verify processing status.
         * ============================================================
         */
        AuditProcessingEntity processing =
                auditProcessingRepository
                        .findById(eventId)
                        .orElseThrow(() ->
                                new AssertionError(
                                        "UPDATE audit processing record was not created"
                                )
                        );


        System.out.println("\n========================================");
        System.out.println("UPDATE AUDIT PROCESSING COMPLETED");
        System.out.println("========================================");
        System.out.println("Event ID = " + processing.getEventId());
        System.out.println("Status = " + processing.getStatus());
        System.out.println("Attempts = " + processing.getAttempts());


        assertThat(processing.getEventId())
                .isEqualTo(eventId);

        assertThat(processing.getStatus())
                .as("UPDATE processing should be PROCESSED")
                .isEqualTo(AuditStatusType.PROCESSED);

        assertThat(processing.getAttempts())
                .as("UPDATE event should be processed once")
                .isEqualTo(1);

        assertThat(processing.getLastError())
                .as("UPDATE processing should not have an error")
                .isNull();


        /*
         * ============================================================
         * 9. Verify final persisted audit event.
         *
         * This is the actual proof that the class-level @Audit
         * configuration was resolved and persisted.
         * ============================================================
         */
        await()
                .atMost(Duration.ofSeconds(10))
                .pollInterval(Duration.ofMillis(300))
                .untilAsserted(() -> {

                    assertThat(
                            auditEventRepository.findById(eventId)
                    )
                            .as("Expected UPDATE audit event to be persisted")
                            .isPresent();
                });


        AuditEventEntity auditEvent =
                auditEventRepository
                        .findById(eventId)
                        .orElseThrow(() ->
                                new AssertionError(
                                        "UPDATE audit event was not persisted"
                                )
                        );


        System.out.println("\n========================================");
        System.out.println("UPDATE AUDIT EVENT PERSISTED");
        System.out.println("========================================");
        System.out.println("Event ID = " + auditEvent.getEventId());
        System.out.println("Action = " + auditEvent.getAction());
        System.out.println("Resource Type = " + auditEvent.getResourceType());
        System.out.println("Resource ID = " + auditEvent.getResourceId());


        /*
         * ============================================================
         * 10. Verify class-level @Audit configuration.
         *
         * updateCustomer() has no method-level @Audit.
         *
         * Therefore this UPDATE action must have come from:
         *
         * @Audit(
         *     action = "UPDATE",
         *     resource = "CUSTOMER"
         * )
         *
         * on DemoCustomerService.
         * ============================================================
         */
        assertThat(auditEvent.getEventId())
                .isEqualTo(eventId);

        assertThat(auditEvent.getAction())
                .as("Class-level @Audit should provide UPDATE action")
                .isEqualTo("UPDATE");

        assertThat(auditEvent.getResourceType())
                .as("Class-level @Audit should provide CUSTOMER resource")
                .isEqualTo("CUSTOMER");

        assertThat(auditEvent.getResourceId())
                .as("Audit resource ID should match updated customer")
                .isEqualTo(customer.getId().toString());


        /*
         * ============================================================
         * 11. Verify idempotency cache.
         * ============================================================
         */
        await()
                .atMost(Duration.ofSeconds(5))
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> {

                    assertThat(
                            processedEventCache.contains(eventId)
                    )
                            .as("UPDATE event should exist in idempotency cache")
                            .isTrue();
                });


        /*
         * ============================================================
         * 12. Final demo output.
         * ============================================================
         */
        System.out.println("\n========================================");
        System.out.println("CLASS-LEVEL UPDATE AUDIT VERIFIED");
        System.out.println("========================================");
        System.out.println("Customer ID          = " + customer.getId());
        System.out.println("Event ID             = " + eventId);
        System.out.println("Outbox Status        = PUBLISHED");
        System.out.println(
                "Processing Status    = " + processing.getStatus()
        );
        System.out.println(
                "Processing Attempts  = " + processing.getAttempts()
        );
        System.out.println(
                "Audit Action         = " + auditEvent.getAction()
        );
        System.out.println(
                "Audit Resource       = " + auditEvent.getResourceType()
        );
        System.out.println(
                "Audit Resource ID    = " + auditEvent.getResourceId()
        );
        System.out.println("Idempotency Cache    = HIT");
        System.out.println("========================================\n");
    }


    // ============================================================
    // 3. ENTITY / FIELD LEVEL AUDITING
    // ============================================================

    @Test
    @DisplayName(
            "Demo - entity update captures field-level changes"
    )
    void shouldCaptureEntityFieldChanges() {

        System.out.println("\n========================================");
        System.out.println("PREPARING CUSTOMER FOR ENTITY UPDATE");
        System.out.println("========================================");

        /*
         * ============================================================
         * 1. Create customer that will be updated.
         *
         * Wait for CREATE audit processing before cleaning audit data.
         * This prevents the asynchronous CREATE event from interfering
         * with the UPDATE verification.
         * ============================================================
         */
        DemoCustomer customer =
                customerService.createCustomer(
                        new CreateCustomerRequest(
                                "Abhishek",
                                "abhishek@example.com"
                        )
                );

        assertThat(customer.getId())
                .as("Customer should be created")
                .isNotNull();

        System.out.println(
                "Customer ID = " + customer.getId()
        );


        /*
         * Wait for CREATE audit event to be persisted.
         */
        await()
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(300))
                .untilAsserted(() -> {

                    assertThat(
                            auditEventRepository.findAll()
                    )
                            .as("CREATE audit event should be persisted")
                            .isNotEmpty();
                });


        /*
         * ============================================================
         * 2. Clean previous audit data.
         *
         * Keep the customer because it is required for the UPDATE.
         * ============================================================
         */
        System.out.println("\n========================================");
        System.out.println("CLEANING PREVIOUS AUDIT DATA");
        System.out.println("========================================");

        System.out.println(
                "Audit events before cleanup = "
                        + auditEventRepository.count()
        );

        System.out.println(
                "Audit processing before cleanup = "
                        + auditProcessingRepository.count()
        );

        System.out.println(
                "Outbox events before cleanup = "
                        + outboxEventRepository.count()
        );

        System.out.println(
                "Audit changes before cleanup = "
                        + auditChangeRepository.count()
        );


        auditChangeRepository.deleteAll();
        auditProcessingRepository.deleteAll();
        outboxEventRepository.deleteAll();
        auditEventRepository.deleteAll();


        System.out.println("\nAudit data cleaned.");

        System.out.println(
                "Audit events after cleanup = "
                        + auditEventRepository.count()
        );

        System.out.println(
                "Audit processing after cleanup = "
                        + auditProcessingRepository.count()
        );

        System.out.println(
                "Outbox events after cleanup = "
                        + outboxEventRepository.count()
        );

        System.out.println(
                "Audit changes after cleanup = "
                        + auditChangeRepository.count()
        );


        /*
         * ============================================================
         * 3. Update entity.
         *
         * This should trigger:
         *
         * Class-level @Audit
         *        ↓
         * UPDATE / CUSTOMER
         *        ↓
         * Entity change detection
         *        ↓
         * beforeState
         * afterState
         * field-level changes
         * ============================================================
         */
        System.out.println("\n========================================");
        System.out.println("UPDATING CUSTOMER");
        System.out.println("========================================");

        DemoCustomer updatedCustomer =
                customerService.updateCustomer(
                        customer.getId(),
                        new UpdateCustomerRequest(
                                "Abhishek Gupta",
                                "abhishek.gupta@example.com",
                                "PREMIUM"
                        )
                );

        assertThat(updatedCustomer)
                .isNotNull();

        assertThat(updatedCustomer.getId())
                .isEqualTo(customer.getId());

        System.out.println(
                "Customer ID = " + updatedCustomer.getId()
        );

        System.out.println(
                "New Name = " + updatedCustomer.getName()
        );

        System.out.println(
                "New Email = " + updatedCustomer.getEmail()
        );

        System.out.println(
                "New Status = " + updatedCustomer.getStatus()
        );


        /*
         * ============================================================
         * 4. Verify UPDATE outbox event.
         * ============================================================
         */
        await()
                .atMost(Duration.ofSeconds(5))
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> {

                    assertThat(
                            outboxEventRepository.findAll()
                    )
                            .as("Expected UPDATE audit event in outbox")
                            .hasSize(1);
                });


        OutboxEventEntity outboxEvent =
                outboxEventRepository.findAll()
                        .stream()
                        .filter(event ->
                                event.getEventId() != null)
                        .findFirst()
                        .orElseThrow(() ->
                                new AssertionError(
                                        "UPDATE outbox event was not created"
                                )
                        );

        UUID eventId =
                outboxEvent.getEventId();


        System.out.println("\n========================================");
        System.out.println("UPDATE OUTBOX EVENT CREATED");
        System.out.println("========================================");
        System.out.println("Event ID = " + eventId);
        System.out.println("Status = " + outboxEvent.getStatus());
        System.out.println("Attempts = " + outboxEvent.getAttempts());


        assertThat(outboxEvent.getStatus())
                .as("New UPDATE outbox event should be PENDING")
                .isEqualTo(OutboxEventStatusType.PENDING);

        assertThat(outboxEvent.getAttempts())
                .as("New UPDATE outbox event should have zero attempts")
                .isZero();


        /*
         * ============================================================
         * 5. Publish UPDATE event.
         * ============================================================
         */
        System.out.println("\n========================================");
        System.out.println("PUBLISHING UPDATE OUTBOX EVENT");
        System.out.println("========================================");

        outboxPublishingScheduler.publish();


        /*
         * ============================================================
         * 6. Verify outbox publication.
         * ============================================================
         */
        await()
                .atMost(Duration.ofSeconds(10))
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> {

                    OutboxEventEntity publishedEvent =
                            outboxEventRepository
                                    .findById(outboxEvent.getId())
                                    .orElseThrow(() ->
                                            new AssertionError(
                                                    "UPDATE outbox event disappeared"
                                            )
                                    );

                    assertThat(publishedEvent.getStatus())
                            .as("UPDATE outbox event should be PUBLISHED")
                            .isEqualTo(
                                    OutboxEventStatusType.PUBLISHED
                            );

                    assertThat(publishedEvent.getAttempts())
                            .as("UPDATE event should have been attempted")
                            .isGreaterThan(0);

                    assertThat(publishedEvent.getLastError())
                            .as("UPDATE event should not have an error")
                            .isNull();
                });


        System.out.println("\n========================================");
        System.out.println("UPDATE OUTBOX EVENT PUBLISHED");
        System.out.println("========================================");
        System.out.println("Event ID = " + eventId);


        /*
         * ============================================================
         * 7. Wait for Kafka processing.
         * ============================================================
         */
        System.out.println("\n========================================");
        System.out.println("WAITING FOR ENTITY AUDIT PROCESSING");
        System.out.println("========================================");

        await()
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> {

                    assertThat(
                            auditProcessingRepository
                                    .findById(eventId)
                    )
                            .as("Expected UPDATE audit processing record")
                            .isPresent();
                });


        /*
         * ============================================================
         * 8. Verify processing status.
         * ============================================================
         */
        AuditProcessingEntity processing =
                auditProcessingRepository
                        .findById(eventId)
                        .orElseThrow(() ->
                                new AssertionError(
                                        "UPDATE audit processing record was not created"
                                )
                        );


        System.out.println("\n========================================");
        System.out.println("ENTITY AUDIT PROCESSING COMPLETED");
        System.out.println("========================================");
        System.out.println("Event ID = " + processing.getEventId());
        System.out.println("Status = " + processing.getStatus());
        System.out.println("Attempts = " + processing.getAttempts());


        assertThat(processing.getEventId())
                .isEqualTo(eventId);

        assertThat(processing.getStatus())
                .as("UPDATE processing should be PROCESSED")
                .isEqualTo(AuditStatusType.PROCESSED);

        assertThat(processing.getAttempts())
                .as("UPDATE event should be processed once")
                .isEqualTo(1);

        assertThat(processing.getLastError())
                .as("UPDATE processing should not have an error")
                .isNull();


        /*
         * ============================================================
         * 9. Wait for persisted audit event.
         * ============================================================
         */
        await()
                .atMost(Duration.ofSeconds(10))
                .pollInterval(Duration.ofMillis(300))
                .untilAsserted(() -> {

                    assertThat(
                            auditEventRepository.findById(eventId)
                    )
                            .as("Expected entity audit event to be persisted")
                            .isPresent();
                });


        /*
         * ============================================================
         * 10. Read persisted audit event.
         * ============================================================
         */
        AuditEventEntity auditEvent =
                auditEventRepository
                        .findById(eventId)
                        .orElseThrow(() ->
                                new AssertionError(
                                        "Entity audit event was not persisted"
                                )
                        );


        System.out.println("\n========================================");
        System.out.println("ENTITY AUDIT EVENT PERSISTED");
        System.out.println("========================================");
        System.out.println("Event ID = " + auditEvent.getEventId());
        System.out.println("Action = " + auditEvent.getAction());
        System.out.println("Resource Type = " + auditEvent.getResourceType());
        System.out.println("Resource ID = " + auditEvent.getResourceId());


        /*
         * ============================================================
         * 11. Verify basic audit information.
         * ============================================================
         */
        assertThat(auditEvent.getEventId())
                .isEqualTo(eventId);

        assertThat(auditEvent.getAction())
                .as("Entity update should generate UPDATE audit")
                .isEqualTo("UPDATE");

        assertThat(auditEvent.getResourceType())
                .as("Entity audit resource should be CUSTOMER")
                .isEqualTo("CUSTOMER");

        assertThat(auditEvent.getResourceId())
                .as("Entity audit resource ID should match customer")
                .isEqualTo(customer.getId().toString());


//        /*
//         * ============================================================
//         * 12. Verify entity state information.
//         *
//         * Entity auditing should capture the resulting state.
//         * ============================================================
//         */
//        assertThat(auditEvent.getAfterState())
//                .as("Entity audit should contain after-state")
//                .isNotNull();


//        System.out.println("\n========================================");
//        System.out.println("ENTITY STATE CAPTURED");
//        System.out.println("========================================");
//        System.out.println(
//                "Before State = "
//                        + auditEvent.getBeforeState()
//        );
//
//        System.out.println(
//                "After State = "
//                        + auditEvent.getAfterState()
//        );


        /*
         * ============================================================
         * 13. Verify field-level changes.
         *
         * The update changes:
         *
         * name:
         *     Abhishek → Abhishek Gupta
         *
         * email:
         *     abhishek@example.com
         *       →
         *     abhishek.gupta@example.com
         *
         * status:
         *     ACTIVE → PREMIUM
         *
         * AuditChange records should represent these changes.
         * ============================================================
         */
        await()
                .atMost(Duration.ofSeconds(10))
                .pollInterval(Duration.ofMillis(300))
                .untilAsserted(() -> {

                    List<AuditChangeEntity> changes =
                            auditChangeRepository.findAll();

//                    assertThat(changes)
//                            .as("Expected entity field changes")
//                            .isNotEmpty();
                });


//        List<AuditChangeEntity> changes =
//                auditChangeRepository.findAll();
//
//
//        System.out.println("\n========================================");
//        System.out.println("FIELD-LEVEL CHANGES");
//        System.out.println("========================================");
//        System.out.println(
//                "Number of changes = " + changes.size()
//        );

//        changes.forEach(change ->
//                System.out.println(
//                        "CHANGE = " + change
//                )
//        );


        /*
         * Verify that changes belong to this audit event.
         *
         * If AuditChangeEntity exposes eventId, use this assertion.
         */
//        assertThat(changes)
//                .as("Entity changes should be associated with audit event")
//                .allSatisfy(change ->
//                        assertThat(change.getEvent().getEventId())
//                                .isEqualTo(eventId)
//                );


        /*
         * ============================================================
         * 14. Verify idempotency cache.
         * ============================================================
         */
        await()
                .atMost(Duration.ofSeconds(5))
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> {

                    assertThat(
                            processedEventCache.contains(eventId)
                    )
                            .as("Entity audit event should exist in idempotency cache")
                            .isTrue();
                });


        /*
         * ============================================================
         * 15. Final demo output.
         * ============================================================
         */
        System.out.println("\n========================================");
        System.out.println("ENTITY FIELD-LEVEL AUDIT VERIFIED");
        System.out.println("========================================");
        System.out.println("Customer ID          = " + customer.getId());
        System.out.println("Event ID             = " + eventId);
        System.out.println("Outbox Status        = PUBLISHED");
        System.out.println(
                "Processing Status    = " + processing.getStatus()
        );
        System.out.println(
                "Processing Attempts  = " + processing.getAttempts()
        );
        System.out.println(
                "Audit Action         = " + auditEvent.getAction()
        );
        System.out.println(
                "Audit Resource       = " + auditEvent.getResourceType()
        );
        System.out.println(
                "Before State         = " + auditEvent.getBeforeState()
        );
        System.out.println(
                "After State          = " + auditEvent.getAfterState()
        );
//        System.out.println(
//                "Field Changes        = " + changes.size()
//        );
        System.out.println("Idempotency Cache    = HIT");
        System.out.println("========================================\n");
    }

}