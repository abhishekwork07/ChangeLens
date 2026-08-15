package io.changelens.outbox;

import io.changelens.core.domain.audit.AuditEventType;
import io.changelens.core.enums.OutboxEventStatusType;
import io.changelens.outbox.entity.OutboxEventEntity;
import io.changelens.outbox.repository.OutboxEventRepository;
import io.changelens.outbox.publisher.service.OutboxClaimService;
import io.changelens.support.IntegrationTestContainers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Import(IntegrationTestContainers.class)
class OutboxClaimServiceIntegrationTest {

    @Autowired
    private OutboxClaimService claimService;

    @Autowired
    private OutboxEventRepository repository;

    @BeforeEach
    void setUp() {
        repository.deleteAll();
    }

    @Test
    void shouldClaimPendingEvents() {

        OutboxEventEntity event = createPendingEvent();

        repository.save(event);

        var claimed = claimService.claimPendingEvents(10);

        assertThat(claimed).hasSize(1);

        OutboxEventEntity persisted =
                repository.findById(event.getId()).orElseThrow();

        assertThat(persisted.getStatus())
                .isEqualTo(OutboxEventStatusType.PROCESSING);

        assertThat(persisted.getAttempts())
                .isEqualTo(1);
    }

    @Test
    void shouldNotClaimAlreadyProcessingEvent() {

        OutboxEventEntity event = createPendingEvent();

        repository.save(event);

        claimService.claimPendingEvents(10);

        var secondClaim =
                claimService.claimPendingEvents(10);

        assertThat(secondClaim).isEmpty();
    }

    private OutboxEventEntity createPendingEvent() {
        return OutboxEventEntity.builder()
                .eventId(UUID.randomUUID())
                .aggregateType("RESOURCE")
                .aggregateId("resource-1")
                .eventType(AuditEventType.UPDATE)
                .payload(Map.of("test", "value"))
                .status(OutboxEventStatusType.PENDING)
                .attempts(0)
                .build();
    }
}