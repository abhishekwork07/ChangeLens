package io.changelens.core.domain.change;

public record AuditChange(
        String fieldPath,
        String fieldName,
        String displayName,
        ChangeType changeType,
        Object oldValue,
        Object newValue
) {
}
