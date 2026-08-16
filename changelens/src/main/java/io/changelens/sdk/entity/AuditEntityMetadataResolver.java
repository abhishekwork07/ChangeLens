package io.changelens.sdk.entity;

import io.changelens.sdk.annotation.Audit;
import org.springframework.stereotype.Component;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

@Component
public class AuditEntityMetadataResolver {

    public AuditableEntityMetadata resolve(Class<?> entityType) {
        Audit entityAudit = entityType.getAnnotation(Audit.class);

        if (entityAudit == null) {
            return null;
        }

        Set<String> auditedFields =
                Arrays.stream(entityType.getDeclaredFields())
                        .filter(field ->
                                field.isAnnotationPresent(
                                        Audit.class
                                ))
                        .map(Field::getName)
                        .collect(Collectors.toSet());

        return new AuditableEntityMetadata(
                entityType,
                entityAudit.resource(),
                auditedFields
        );
    }

    public boolean isAuditable(Class<?> entityType) {
        return entityType.isAnnotationPresent(
                Audit.class
        );
    }
}
