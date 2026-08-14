package io.changelens.storage.mapper;

import io.changelens.core.domain.change.AuditChange;
import io.changelens.storage.entity.AuditChangeEntity;
import io.changelens.storage.entity.AuditEventEntity;
import org.springframework.stereotype.Component;

@Component
public class AuditChangeMapper {

    public AuditChangeEntity toEntity(AuditChange change,
            AuditEventEntity eventEntity) {

        return AuditChangeEntity.builder()
                .event(eventEntity)
                .fieldPath(change.fieldPath())
                .fieldName(change.fieldName())
                .displayName(change.displayName())
                .changeType(change.changeType())
                .oldValue(change.oldValue())
                .newValue(change.newValue())
                .build();
    }

    public AuditChange toDomain(AuditChangeEntity entity) {
        return new AuditChange(
                entity.getFieldPath(),
                entity.getFieldName(),
                entity.getDisplayName(),
                entity.getChangeType(),
                entity.getOldValue(),
                entity.getNewValue()
        );
    }
}
