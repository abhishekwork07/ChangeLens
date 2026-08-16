package io.changelens.processing.service;

import io.changelens.cache.ProcessedEventCache;
import io.changelens.core.domain.audit.AuditEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
@RequiredArgsConstructor
public class AuditProcessingService {

    private final AuditProcessingStatusService statusService;
    private final ProcessedEventCache processedEventCache;

    public void process(AuditEvent event) {
        statusService.markProcessed(event);
        processedEventCache.put(event.eventId());
    }

    public void markFailed(UUID eventId, String error) {
        statusService.markFailed(eventId, error);
    }
}
