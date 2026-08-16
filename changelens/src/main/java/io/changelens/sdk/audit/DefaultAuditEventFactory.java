package io.changelens.sdk.audit;

import io.changelens.core.context.AuditContext;
import io.changelens.core.domain.actor.Actor;
import io.changelens.core.domain.audit.AuditEvent;
import io.changelens.core.domain.audit.AuditEventType;
import io.changelens.core.domain.change.AuditChange;
import io.changelens.core.domain.change.ChangeSet;
import io.changelens.core.domain.change.ChangeType;
import io.changelens.core.domain.resource.Resource;
import io.changelens.processing.service.AuditProcessingException;
import io.changelens.sdk.annotation.Audit;
import io.changelens.sdk.audit.provider.AuditActorProvider;
import io.changelens.sdk.audit.provider.AuditResourceProvider;
import io.changelens.sdk.audit.provider.AuditTenantProvider;
import io.changelens.sdk.context.AuditContextProvider;
import io.changelens.sdk.entity.FieldChange;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Component
@RequiredArgsConstructor
public class DefaultAuditEventFactory implements AuditEventFactory {

    private final AuditContextProvider contextProvider;
    private final AuditActorProvider actorProvider;
    private final AuditTenantProvider tenantProvider;
    private final AuditResourceProvider resourceProvider;

    @Override
    public AuditEvent create(AuditCaptureContext captureContext) {

        Audit audit = captureContext.audit();
        AuditContext context = contextProvider.getContext();
        Actor actor = actorProvider.getActor();
        String tenantId = tenantProvider.getTenantId();
        Resource resource = resourceProvider.getResource(captureContext);
        ChangeSet changeSet = createChangeSet(captureContext);
        String action = resolveAction(audit, captureContext.method());

        return new AuditEvent(
                UUID.randomUUID(),
                1,
                tenantId,
                resolveEventType(action),
                Instant.now(),
                action,
                actor,
                resource,
                changeSet,
                null,
                null,
                context,
                Map.of()
        );
    }

    private AuditEventType resolveEventType(String action) {
        try {
            return AuditEventType.valueOf(
                    action.toUpperCase()
            );
        } catch (IllegalArgumentException exception) {
            throw new AuditProcessingException(
                    "Unsupported audit action: " + action, exception);
        }
    }

    private ChangeSet createChangeSet(AuditCaptureContext captureContext) {
        List<AuditChange> changes =
                captureContext.fieldChanges()
                        .stream()
                        .map(this::toAuditChange)
                        .toList();

        String summary =
                captureContext.source() == AuditSource.ENTITY
                        ? "Entity fields updated"
                        : captureContext.audit().action();

        return new ChangeSet(summary, changes);
    }

    private AuditChange toAuditChange(FieldChange fieldChange) {
        String fieldName = fieldChange.fieldName();

        return new AuditChange(
                fieldName,
                fieldName,
                fieldName,
                resolveChangeType(
                        fieldChange.previousValue(),
                        fieldChange.currentValue()
                ),
                fieldChange.previousValue(),
                fieldChange.currentValue()
        );
    }

    private ChangeType resolveChangeType(Object oldValue, Object newValue) {
        if (oldValue == null && newValue != null) {
            return ChangeType.ADDED;
        }
        if (oldValue != null && newValue == null) {
            return ChangeType.REMOVED;
        }
        return ChangeType.UPDATED;
    }

    private String resolveAction(Audit audit, Method method) {
        if (!audit.action().isBlank()) {
            return audit.action();
        }
        return method.getName().toUpperCase();
    }
}
