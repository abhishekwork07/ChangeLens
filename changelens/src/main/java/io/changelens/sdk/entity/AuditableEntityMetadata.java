package io.changelens.sdk.entity;

import java.util.Set;

public record AuditableEntityMetadata(
        Class<?> entityType,
        String resource,
        Set<String> auditedFields
) {
}
