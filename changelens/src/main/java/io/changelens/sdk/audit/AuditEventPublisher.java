package io.changelens.sdk.audit;

import io.changelens.core.domain.audit.AuditEvent;

public interface AuditEventPublisher {

    void publish(AuditEvent event);
}
