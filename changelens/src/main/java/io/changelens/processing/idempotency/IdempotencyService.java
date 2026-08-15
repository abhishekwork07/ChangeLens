package io.changelens.processing.idempotency;

import io.changelens.processing.ProcessingClaimResult;
import io.changelens.processing.service.AuditProcessingException;
import io.changelens.storage.entity.AuditProcessingEntity;
import io.changelens.storage.repository.AuditProcessingRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class IdempotencyService {

    private final AuditProcessingRepository auditProcessingRepository;

    @Value("${changelens.processing.retry.max-attempts:3}")
    private int maxAttempts;

    @Value("${changelens.processing.processing-timeout:5m}")
    private Duration processingTimeout;

    @Transactional
    public ProcessingClaimResult tryClaim(UUID eventId) {

        Optional<AuditProcessingEntity> existing =
                auditProcessingRepository.findById(eventId);

        if (existing.isEmpty()) {
            return startProcessing(eventId);
        }

        AuditProcessingEntity processing = existing.get();

        return switch (processing.getStatus()) {
            case PROCESSED ->
                    ProcessingClaimResult.ALREADY_PROCESSED;

            case PROCESSING ->
                    handleProcessingState(processing);

            case FAILED ->
                    retryFailedEvent(processing);

            default ->
                    throw new AuditProcessingException(
                            "Unsupported audit processing status: "
                                    + processing.getStatus()
                    );
        };
    }

    private ProcessingClaimResult startProcessing(UUID eventId) {
        Instant now = Instant.now();

        int inserted = auditProcessingRepository
                .tryStartProcessing(eventId, now, now, now);

        if (inserted == 1) {
            return ProcessingClaimResult.STARTED;
        }

        // Another consumer claimed the event concurrently.
        return resolveConcurrentClaim(eventId);
    }

    private ProcessingClaimResult handleProcessingState(AuditProcessingEntity processing) {

        /*
         * Maximum attempts always takes precedence.
         * The event cannot be retried regardless of whether
         * the current processing lease is stale.
         */
        if (processing.getAttempts() >= maxAttempts) {
            return ProcessingClaimResult.MAX_ATTEMPTS_REACHED;
        }

        Instant now = Instant.now();
        Instant staleThreshold = now.minus(processingTimeout);

        if (!processing.getUpdatedAt().isBefore(staleThreshold)) {
            return ProcessingClaimResult.ALREADY_PROCESSING;
        }

        int updated = auditProcessingRepository
                .reclaimStaleProcessing(
                        processing.getEventId(),
                        staleThreshold,
                        now,
                        maxAttempts
                );

        if (updated == 1) {
            return ProcessingClaimResult.RETRY_STARTED;
        }

        return resolveConcurrentClaim(processing.getEventId());
    }

    private ProcessingClaimResult retryFailedEvent(
            AuditProcessingEntity processing) {

        if (processing.getAttempts() >= maxAttempts) {
            return ProcessingClaimResult.MAX_ATTEMPTS_REACHED;
        }

        Instant now = Instant.now();

        int updated = auditProcessingRepository
                .retryProcessing(processing.getEventId(), now, maxAttempts);

        if (updated == 1) {
            return ProcessingClaimResult.RETRY_STARTED;
        }

        return resolveConcurrentClaim(processing.getEventId());
    }

    private ProcessingClaimResult resolveConcurrentClaim(UUID eventId) {

        AuditProcessingEntity current =
                auditProcessingRepository.findById(eventId)
                        .orElseThrow(() ->
                                new AuditProcessingException(
                                        "Unable to determine processing state: "
                                                + eventId
                                ));

        return switch (current.getStatus()) {
            case PROCESSED ->
                    ProcessingClaimResult.ALREADY_PROCESSED;

            case PROCESSING ->
                    ProcessingClaimResult.ALREADY_PROCESSING;

            case FAILED ->
                    current.getAttempts() >= maxAttempts
                            ? ProcessingClaimResult.MAX_ATTEMPTS_REACHED
                            : ProcessingClaimResult.ALREADY_PROCESSING;

            default ->
                    throw new AuditProcessingException(
                            "Unsupported audit processing status: "
                                    + current.getStatus()
                    );
        };
    }
}
