package io.changelens.outbox.repository;

import io.changelens.outbox.entity.OutboxEventEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public interface OutboxEventRepository extends JpaRepository<OutboxEventEntity, Long> {

    @Query(value = """
            SELECT *
            FROM outbox_event
            WHERE status = 'PENDING'
            ORDER BY created_at
            FOR UPDATE SKIP LOCKED
            LIMIT :batchSize
            """, nativeQuery = true)
    List<OutboxEventEntity> findPendingEventsForPublishing(@Param("batchSize") int batchSize
    );

    @Modifying
    @Query(value = """
        UPDATE outbox_event
        SET status = 'PUBLISHED',
            published_at = :publishedAt,
            updated_at = :updatedAt,
            last_error = NULL
        WHERE event_id = :eventId
          AND status = 'PROCESSING'
        """, nativeQuery = true)
    int updateEventAsPublished(
            @Param("eventId") UUID eventId,
            @Param("publishedAt") Instant publishedAt,
            @Param("updatedAt") Instant updatedAt
    );

    @Modifying
    @Query(value = """
        UPDATE outbox_event
        SET status = 'FAILED',
            last_error = :lastError,
            updated_at = :updatedAt
        WHERE event_id = :eventId
          AND status = 'PROCESSING'
        """, nativeQuery = true)
    int updateEventAsFailed(
            @Param("eventId") UUID eventId,
            @Param("lastError") String lastError,
            @Param("updatedAt") Instant updatedAt
    );

    @Modifying
    @Query(value = """
        UPDATE outbox_event
        SET status = 'PENDING',
            updated_at = :updatedAt
        WHERE status = 'PROCESSING'
          AND updated_at < :threshold
        """, nativeQuery = true)
    int recoverStaleProcessingEvents(
            @Param("threshold") Instant threshold,
            @Param("updatedAt") Instant updatedAt
    );

    OutboxEventEntity findByEventId(UUID eventId);
}
