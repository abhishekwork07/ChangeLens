package io.changelens.core.domain.audit;

import io.changelens.core.context.AuditContext;
import io.changelens.core.domain.actor.Actor;
import io.changelens.core.domain.change.ChangeSet;
import io.changelens.core.domain.resource.Resource;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public record AuditEvent(
        UUID eventId,
        int eventVersion,
        String tenantId,
        AuditEventType eventType,
        Instant timestamp,
        String action,
        Actor actor,
        Resource resource,
        ChangeSet changeSet,
        Object beforeState,
        Object afterState,
        AuditContext context,
        Map<String, Object> extensions
) {

    public AuditEvent {
        extensions = extensions == null
                ? Map.of()
                : Map.copyOf(extensions);
    }
}
