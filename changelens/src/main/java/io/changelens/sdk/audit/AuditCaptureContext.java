package io.changelens.sdk.audit;

import io.changelens.sdk.annotation.Audit;
import io.changelens.sdk.entity.FieldChange;

import java.lang.reflect.Method;
import java.util.List;

public record AuditCaptureContext(
        Audit audit,
        AuditSource source,
        Method method,
        Object target,
        Object[] arguments,
        Object result,
        List<FieldChange> fieldChanges
) {
}