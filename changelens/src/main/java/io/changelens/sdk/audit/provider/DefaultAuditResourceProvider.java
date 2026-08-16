package io.changelens.sdk.audit.provider;

import io.changelens.core.domain.resource.Resource;
import io.changelens.sdk.annotation.Audit;
import io.changelens.sdk.audit.AuditCaptureContext;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;

import java.lang.reflect.Method;

@ConditionalOnMissingBean(AuditResourceProvider.class)
public class DefaultAuditResourceProvider implements AuditResourceProvider {

    @Override
    public Resource getResource(AuditCaptureContext context) {
        Audit audit = context.audit();
        Object resourceObject = resolveResourceObject(context);

        if (resourceObject == null) {
            return new Resource(
                    audit.resource(),
                    null,
                    audit.resource()
            );
        }

        return new Resource(
                audit.resource(),
                resolveResourceId(resourceObject),
                resolveResourceName(resourceObject)
        );
    }

    private Object resolveResourceObject(AuditCaptureContext context) {
        if (context.result() != null) {
            return context.result();
        }

        if (context.arguments() != null) {
            for (Object argument : context.arguments()) {
                if (argument != null) {
                    return argument;
                }
            }
        }

        return null;
    }

    private String resolveResourceId(Object object) {

        try {
            Method method = object.getClass().getMethod("getId");
            Object id = method.invoke(object);

            return id != null
                    ? id.toString()
                    : null;

        } catch (ReflectiveOperationException exception) {
            return null;
        }
    }

    private String resolveResourceName(Object object) {

        try {
            Method method = object.getClass().getMethod("getName");

            Object name = method.invoke(object);

            return name != null
                    ? name.toString()
                    : object.getClass().getSimpleName();

        } catch (ReflectiveOperationException exception) {
            return object.getClass().getSimpleName();
        }
    }
}
