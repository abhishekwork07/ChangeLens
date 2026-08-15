package io.changelens.outbox;

import io.changelens.core.domain.audit.AuditEventType;
import io.changelens.core.enums.OutboxEventStatusType;
import io.changelens.outbox.entity.OutboxEventEntity;
import io.changelens.outbox.publisher.service.OutboxClaimService;
import io.changelens.outbox.repository.OutboxEventRepository;
import io.changelens.support.IntegrationTestContainers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Import(IntegrationTestContainers.class)
class OutboxConcurrencyIntegrationTest {

    @Autowired
    private OutboxClaimService claimService;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @BeforeEach
    void setUp() {
        outboxEventRepository.deleteAll();
    }

    @Test
    void shouldNotClaimSameEventsWhenPublishersRunConcurrently()
            throws Exception {

        // Given
        int eventCount = 10;
        int batchSize = 10;

        List<UUID> eventIds = createPendingEvents(eventCount);

        ExecutorService executor =
                Executors.newFixedThreadPool(2);

        CountDownLatch startLatch =
                new CountDownLatch(1);

        try {
            // When
            Future<List<OutboxEventEntity>> publisherOne =
                    executor.submit(() -> {
                        startLatch.await();
                        return claimService.claimPendingEvents(batchSize);
                    });

            Future<List<OutboxEventEntity>> publisherTwo =
                    executor.submit(() -> {
                        startLatch.await();
                        return claimService.claimPendingEvents(batchSize);
                    });

            // Start both publishers at approximately the same time.
            startLatch.countDown();

            List<OutboxEventEntity> claimedByPublisherOne =
                    publisherOne.get(10, TimeUnit.SECONDS);

            List<OutboxEventEntity> claimedByPublisherTwo =
                    publisherTwo.get(10, TimeUnit.SECONDS);

            // Then
            List<UUID> claimedEventIds = new java.util.ArrayList<>();

            claimedEventIds.addAll(
                    claimedByPublisherOne.stream()
                            .map(OutboxEventEntity::getEventId)
                            .toList()
            );

            claimedEventIds.addAll(
                    claimedByPublisherTwo.stream()
                            .map(OutboxEventEntity::getEventId)
                            .toList()
            );

            // Every event should be claimed exactly once.
            assertThat(claimedEventIds)
                    .hasSize(eventCount);

            assertThat(claimedEventIds)
                    .doesNotHaveDuplicates();

            assertThat(claimedEventIds)
                    .containsExactlyInAnyOrderElementsOf(eventIds);

            // Every event must now be PROCESSING.
            List<OutboxEventEntity> persistedEvents =
                    outboxEventRepository.findAll();

            assertThat(persistedEvents)
                    .hasSize(eventCount);

            assertThat(persistedEvents)
                    .allMatch(event ->
                            event.getStatus()
                                    == OutboxEventStatusType.PROCESSING);

            assertThat(persistedEvents)
                    .allMatch(event ->
                            event.getAttempts() == 1);

        } finally {
            executor.shutdownNow();
        }
    }

    private List<UUID> createPendingEvents(int count) {

        List<OutboxEventEntity> events =
                java.util.stream.IntStream.range(0, count)
                        .mapToObj(index ->
                                OutboxEventEntity.builder()
                                        .eventId(UUID.randomUUID())
                                        .aggregateType("RESOURCE")
                                        .aggregateId(
                                                "resource-" + index
                                        )
                                        .eventType(AuditEventType.UPDATE)
                                        .payload(
                                                Map.of(
                                                        "index",
                                                        index
                                                )
                                        )
                                        .status(
                                                OutboxEventStatusType.PENDING
                                        )
                                        .attempts(0)
                                        .build()
                        )
                        .toList();

        return outboxEventRepository.saveAll(events)
                .stream()
                .map(OutboxEventEntity::getEventId)
                .toList();
    }
}