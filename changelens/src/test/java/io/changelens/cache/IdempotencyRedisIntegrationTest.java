package io.changelens.cache;

import io.changelens.core.context.AuditContext;
import io.changelens.core.domain.actor.Actor;
import io.changelens.core.domain.actor.ActorType;
import io.changelens.core.domain.audit.AuditEvent;
import io.changelens.core.domain.audit.AuditEventType;
import io.changelens.core.domain.change.ChangeSet;
import io.changelens.core.domain.resource.Resource;
import io.changelens.core.enums.AuditStatusType;
import io.changelens.processing.ProcessingClaimResult;
import io.changelens.processing.idempotency.IdempotencyService;
import io.changelens.storage.entity.AuditEventEntity;
import io.changelens.storage.entity.AuditProcessingEntity;
import io.changelens.storage.mapper.AuditEventMapper;
import io.changelens.storage.repository.AuditEventRepository;
import io.changelens.storage.repository.AuditProcessingRepository;
import io.changelens.support.IntegrationTestContainers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Import(IntegrationTestContainers.class)
class IdempotencyRedisIntegrationTest {

    private static final String REDIS_KEY_PREFIX =
            "changelens:processed:";

    @Autowired
    private IdempotencyService idempotencyService;

    @Autowired
    private ProcessedEventCache processedEventCache;

    @Autowired
    private AuditProcessingRepository auditProcessingRepository;

    @Autowired
    private AuditEventRepository auditEventRepository;

    @Autowired
    private AuditEventMapper auditEventMapper;

    @Autowired
    private StringRedisTemplate redisTemplate;

    @BeforeEach
    void setUp() {

        auditProcessingRepository.deleteAll();
        auditEventRepository.deleteAll();

        var keys =
                redisTemplate.keys(
                        REDIS_KEY_PREFIX + "*"
                );

        if (keys != null && !keys.isEmpty()) {
            redisTemplate.delete(keys);
        }
    }

    @Test
    void shouldStartProcessingWhenRedisCacheMisses() {

        UUID eventId = UUID.randomUUID();

        /*
         * audit_processing.event_id has a foreign key
         * to audit_event.event_id.
         */
        createAuditEvent(eventId);

        assertThat(
                processedEventCache.contains(eventId)
        ).isFalse();

        ProcessingClaimResult result =
                idempotencyService.tryClaim(eventId);

        assertThat(result)
                .isEqualTo(
                        ProcessingClaimResult.STARTED
                );

        AuditProcessingEntity processing =
                auditProcessingRepository
                        .findById(eventId)
                        .orElseThrow();

        assertThat(processing.getStatus())
                .isEqualTo(
                        AuditStatusType.PROCESSING
                );

        assertThat(processing.getAttempts())
                .isEqualTo(1);
    }

    @Test
    void shouldFallbackToDatabaseWhenRedisMissesForProcessedEvent() {

        UUID eventId = UUID.randomUUID();

        /*
         * Parent audit event is required because
         * audit_processing.event_id references
         * audit_event.event_id.
         */
        createAuditEvent(eventId);

        Instant now = Instant.now();

        AuditProcessingEntity processing =
                AuditProcessingEntity.builder()
                        .eventId(eventId)
                        .status(AuditStatusType.PROCESSED)
                        .attempts(1)
                        .receivedAt(now)
                        .processedAt(now)
                        .lastError(null)
                        .createdAt(now)
                        .updatedAt(now)
                        .build();

        auditProcessingRepository
                .saveAndFlush(processing);

        /*
         * Make sure Redis does not contain the event.
         */
        processedEventCache.evict(eventId);

        assertThat(
                processedEventCache.contains(eventId)
        ).isFalse();

        /*
         * Redis MISS → PostgreSQL → PROCESSED.
         */
        ProcessingClaimResult result =
                idempotencyService.tryClaim(eventId);

        assertThat(result)
                .isEqualTo(
                        ProcessingClaimResult.ALREADY_PROCESSED
                );

        /*
         * IdempotencyService should refresh the
         * Redis cache after discovering PROCESSED
         * from PostgreSQL.
         */
        assertThat(
                processedEventCache.contains(eventId)
        ).isTrue();
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