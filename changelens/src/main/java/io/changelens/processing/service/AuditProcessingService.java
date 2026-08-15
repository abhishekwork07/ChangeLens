package io.changelens.processing.service;

import io.changelens.core.domain.audit.AuditEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Service
@RequiredArgsConstructor
public class AuditProcessingService {

    private final AuditProcessingStatusService statusService;

    @Transactional
    public void process(AuditEvent event) {
        statusService.markProcessed(event.eventId());
    }

    @Transactional
    public void markFailed(UUID eventId, String error) {
        statusService.markFailed(eventId, error);
    }
}
