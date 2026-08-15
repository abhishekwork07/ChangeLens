package io.changelens.processing.processor;

import io.changelens.core.domain.audit.AuditEvent;
import io.changelens.core.validation.AuditEventValidator;
import io.changelens.processing.ProcessingClaimResult;
import io.changelens.processing.dlq.AuditDlqService;
import io.changelens.processing.service.AuditProcessingService;
import io.changelens.processing.idempotency.IdempotencyService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class AuditEventProcessor {

    private final AuditEventDeserializer deserializer;
    private final AuditEventValidator validator;
    private final IdempotencyService idempotencyService;
    private final AuditProcessingService processingService;
    private final AuditDlqService dlqService;

    public void process(String payload) {
        AuditEvent event = deserializer.deserialize(payload);
        validator.validate(event);

        ProcessingClaimResult result = idempotencyService
                .tryClaim(event.eventId());

        switch (result) {

            case STARTED, RETRY_STARTED -> {
                try {
                    processingService.process(event);
                } catch (Exception exception) {

                    processingService.markFailed(
                            event.eventId(),
                            exception.getMessage()
                    );

                    throw exception;
                }
            }

            case ALREADY_PROCESSED, ALREADY_PROCESSING -> {
                // ignore
            }

            case MAX_ATTEMPTS_REACHED ->
                // DLQ handling
                    dlqService.moveToDlq(event.eventId(),
                            null, "Maximum processing attempts exceeded");
        }
    }
}
