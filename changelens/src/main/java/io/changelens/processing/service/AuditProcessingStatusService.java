package io.changelens.processing.service;

import io.changelens.storage.repository.AuditProcessingRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class AuditProcessingStatusService {

    private final AuditProcessingRepository auditProcessingRepository;

    @Transactional
    public void markProcessed(UUID eventId) {
        Instant now = Instant.now();
        int updated = auditProcessingRepository
                .markProcessed(eventId, now, now);

        if (updated != 1) {
            throw new AuditProcessingException(
                    "Unable to mark audit event as PROCESSED: " + eventId);
        }
    }

    @Transactional
    public int markFailed(UUID eventId, String error) {
        Instant now = Instant.now();
        return auditProcessingRepository
                .markFailed(eventId, error, now);
    }

}
