package io.changelens.storage.repository;

import io.changelens.storage.entity.AuditProcessingEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.UUID;

public interface AuditProcessingRepository extends JpaRepository<AuditProcessingEntity, UUID> {

    @Modifying
    @Query(value = """
    INSERT INTO audit_processing (
        event_id,
        status,
        attempts,
        received_at,
        created_at,
        updated_at
    )
    VALUES (
        :eventId,
        'PROCESSING',
        1,
        :receivedAt,
        :createdAt,
        :updatedAt
    )
    ON CONFLICT (event_id) DO NOTHING
    """, nativeQuery = true)
    int tryStartProcessing(
            @Param("eventId") UUID eventId,
            @Param("receivedAt") Instant receivedAt,
            @Param("createdAt") Instant createdAt,
            @Param("updatedAt") Instant updatedAt
    );

    @Modifying
    @Query(value = """
    UPDATE audit_processing
    SET status = 'PROCESSED',
        processed_at = :processedAt,
        updated_at = :updatedAt,
        last_error = NULL
    WHERE event_id = :eventId
      AND status = 'PROCESSING'
    """, nativeQuery = true)
    int markProcessed(
            @Param("eventId") UUID eventId,
            @Param("processedAt") Instant processedAt,
            @Param("updatedAt") Instant updatedAt
    );

    @Modifying
    @Query(value = """
    UPDATE audit_processing
    SET status = 'FAILED',
        last_error = :lastError,
        updated_at = :updatedAt
    WHERE event_id = :eventId
      AND status = 'PROCESSING'
    """, nativeQuery = true)
    int markFailed(
            @Param("eventId") UUID eventId,
            @Param("lastError") String lastError,
            @Param("updatedAt") Instant updatedAt
    );

    @Modifying
    @Query(value = """
        UPDATE audit_processing
        SET status = 'PROCESSING',
            attempts = attempts + 1,
            last_error = NULL,
            updated_at = :updatedAt
        WHERE event_id = :eventId
          AND status = 'FAILED'
          AND attempts < :maxAttempts
        """, nativeQuery = true)
    int retryProcessing(
            @Param("eventId") UUID eventId,
            @Param("updatedAt") Instant updatedAt,
            @Param("maxAttempts") int maxAttempts
    );

    @Modifying
    @Query(value = """
        UPDATE audit_processing
        SET status = 'PROCESSING',
            attempts = attempts + 1,
            last_error = NULL,
            updated_at = :updatedAt
        WHERE event_id = :eventId
          AND status = 'PROCESSING'
          AND updated_at < :staleThreshold
          AND attempts < :maxAttempts
        """, nativeQuery = true)
    int reclaimStaleProcessing(
            @Param("eventId") UUID eventId,
            @Param("staleThreshold") Instant staleThreshold,
            @Param("updatedAt") Instant updatedAt,
            @Param("maxAttempts") int maxAttempts
    );

    @Modifying
    @Query(value = """
    UPDATE audit_processing
    SET updated_at = :updatedAt
    WHERE event_id = :eventId
    """, nativeQuery = true)
    int updateUpdatedAt(
            @Param("eventId") UUID eventId,
            @Param("updatedAt") Instant updatedAt
    );
}
