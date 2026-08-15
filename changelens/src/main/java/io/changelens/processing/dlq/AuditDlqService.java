package io.changelens.processing.dlq;

import io.changelens.core.enums.DlqStatusType;
import io.changelens.processing.service.AuditProcessingException;
import io.changelens.processing.service.AuditProcessingStatusService;
import io.changelens.storage.entity.AuditDlqEntity;
import io.changelens.storage.entity.AuditProcessingEntity;
import io.changelens.storage.repository.AuditDlqRepository;
import io.changelens.storage.repository.AuditProcessingRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class AuditDlqService {

    private final AuditProcessingRepository auditProcessingRepository;
    private final AuditDlqRepository auditDlqRepository;
    private final AuditProcessingStatusService statusService;

    @Transactional
    public void moveToDlq(UUID eventId, String payload, String errorMessage) {

        Instant now = Instant.now();

        AuditProcessingEntity processing =
                auditProcessingRepository.findById(eventId)
                        .orElseThrow(() ->
                                new AuditProcessingException(
                                        "Audit processing record not found: "
                                                + eventId));

        String normalizedError = normalizeErrorMessage(errorMessage);

        int updated = statusService.markFailed(eventId, normalizedError);

        if (updated != 1) {
            throw new AuditProcessingException(
                    "Unable to mark audit event as FAILED: "
                            + eventId);
        }

        AuditDlqEntity dlqEntity = AuditDlqEntity.builder()
                .eventId(eventId)
                .status(DlqStatusType.FAILED)
                .attempts(processing.getAttempts())
                .payload(payload != null ? payload : "")
                .errorMessage(normalizedError)
                .failedAt(now)
                .build();

        auditDlqRepository.save(dlqEntity);
    }

    @Transactional
    public void moveRawPayloadToDlq(String payload, String errorMessage) {
        Instant now = Instant.now();

        AuditDlqEntity dlqEntity = AuditDlqEntity.builder()
                .eventId(UUID.randomUUID())
                .status(DlqStatusType.FAILED)
                .attempts(1)
                .payload(payload != null ? payload : "")
                .errorMessage(
                        errorMessage != null && !errorMessage.isBlank()
                                ? errorMessage
                                : "Unknown Kafka processing error"
                )
                .failedAt(now)
                .build();

        auditDlqRepository.save(dlqEntity);
    }

    private String normalizeErrorMessage(String errorMessage) {
        if (errorMessage == null || errorMessage.isBlank()) {
            return "Unknown Kafka processing error";
        }

        return errorMessage.length() > 2048
                ? errorMessage.substring(0, 2048)
                : errorMessage;
    }
}
