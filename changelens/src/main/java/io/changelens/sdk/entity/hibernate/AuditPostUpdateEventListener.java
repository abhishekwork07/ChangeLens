package io.changelens.sdk.entity.hibernate;

import io.changelens.core.domain.audit.AuditEvent;
import io.changelens.sdk.audit.AuditCaptureContext;
import io.changelens.sdk.audit.AuditEventFactory;
import io.changelens.sdk.audit.AuditEventPublisher;
import io.changelens.sdk.audit.AuditSource;
import io.changelens.sdk.annotation.Audit;
import io.changelens.sdk.entity.AuditEntityMetadataResolver;
import io.changelens.sdk.entity.AuditableEntityMetadata;
import io.changelens.sdk.entity.FieldChange;
import lombok.RequiredArgsConstructor;
import org.hibernate.event.spi.PostUpdateEvent;
import org.hibernate.event.spi.PostUpdateEventListener;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@RequiredArgsConstructor
public class AuditPostUpdateEventListener implements PostUpdateEventListener {

    private final AuditEntityMetadataResolver metadataResolver;
    private final AuditEventFactory auditEventFactory;
    private final AuditEventPublisher auditEventPublisher;

    @Override
    public void onPostUpdate(PostUpdateEvent event) {
        Object entity = event.getEntity();
        Class<?> entityType = entity.getClass();

        if (!metadataResolver.isAuditable(entityType)) {
            return;
        }

        AuditableEntityMetadata metadata = metadataResolver.resolve(entityType);

        if (metadata == null || metadata.auditedFields().isEmpty()) {
            return;
        }

        List<FieldChange> changes = extractFieldChanges(
                        event, metadata);

        if (changes.isEmpty()) {
            return;
        }

        Audit audit = entityType.getAnnotation(Audit.class);

        AuditCaptureContext context =
                new AuditCaptureContext(
                        audit,
                        AuditSource.ENTITY,
                        null,
                        entity,
                        null,
                        entity,
                        changes
                );

        AuditEvent auditEvent = auditEventFactory.create(context);
        auditEventPublisher.publish(auditEvent);
    }

    private List<FieldChange> extractFieldChanges(PostUpdateEvent event,
            AuditableEntityMetadata metadata) {

        String[] propertyNames = event.getPersister().getPropertyNames();
        Object[] oldState = event.getOldState();
        Object[] newState = event.getState();

        if (oldState == null || newState == null) {
            return List.of();
        }

        List<FieldChange> changes = new ArrayList<>();

        for (int index = 0; index < propertyNames.length; index++) {
            String propertyName = propertyNames[index];

            if (!metadata.auditedFields().contains(propertyName)) {
                continue;
            }

            Object oldValue = oldState[index];
            Object newValue = newState[index];

            if (!Objects.equals(oldValue, newValue)) {
                changes.add(new FieldChange(propertyName, oldValue, newValue)
                );
            }
        }

        return changes;
    }

    @Override
    public boolean requiresPostCommitHandling(
            org.hibernate.persister.entity.EntityPersister persister) {
        return false;
    }
}
