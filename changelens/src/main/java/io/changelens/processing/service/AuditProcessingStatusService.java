package io.changelens.processing.service;

import io.changelens.core.application.AuditIngestionService;
import io.changelens.core.domain.audit.AuditEvent;
import io.changelens.core.domain.change.ChangeSet;
import io.changelens.storage.entity.AuditEventEntity;
import io.changelens.storage.mapper.AuditEventMapper;
import io.changelens.storage.repository.AuditChangeRepository;
import io.changelens.storage.repository.AuditEventRepository;
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
    private final AuditEventRepository auditEventRepository;
    private final AuditEventMapper auditEventMapper;
    private final AuditIngestionService auditIngestionService;

    @Transactional
    public void markProcessed(AuditEvent event) {
        Instant now = Instant.now();
        UUID eventId = event.eventId();

        int updated = auditProcessingRepository
                .markProcessed(eventId, now, now);

        if (updated != 1) {
            throw new AuditProcessingException(
                    "Unable to mark audit event as PROCESSED: " + eventId);
        }

//        auditEventRepository.save(auditEventMapper.toEntity(event));
        auditIngestionService.ingest(event);
    }

    @Transactional
    public int markFailed(UUID eventId, String error) {
        Instant now = Instant.now();
        return auditProcessingRepository
                .markFailed(eventId, error, now);
    }

}
