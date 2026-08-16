package io.changelens.outbox.publisher;

import io.changelens.core.domain.audit.AuditEvent;
import io.changelens.outbox.publisher.service.OutboxEventWriter;
import io.changelens.sdk.audit.AuditEventPublisher;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class OutboxAuditEventPublisher implements AuditEventPublisher {

    private final OutboxEventWriter outboxEventWriter;

    @Override
    public void publish(AuditEvent event) {
        outboxEventWriter.write(event);
    }
}