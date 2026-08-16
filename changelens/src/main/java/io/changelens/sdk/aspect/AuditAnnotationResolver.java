package io.changelens.sdk.aspect;

import io.changelens.sdk.annotation.Audit;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;

@Component
public class AuditAnnotationResolver {

    public Audit resolve(Method method, Class<?> targetClass) {
        Audit methodAudit = method.getAnnotation(Audit.class);

        if (methodAudit != null) {
            return methodAudit;
        }

        return targetClass.getAnnotation(Audit.class);
    }
}
