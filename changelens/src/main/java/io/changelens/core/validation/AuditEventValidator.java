package io.changelens.core.validation;

import io.changelens.core.context.AuditContext;
import io.changelens.core.domain.actor.Actor;
import io.changelens.core.domain.audit.AuditEvent;
import io.changelens.core.domain.resource.Resource;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

@Component
public class AuditEventValidator {

    public void validate(AuditEvent event) {
        if (event == null) {
            throw new AuditValidationException(
                    "Audit event must not be null"
            );
        }

        validateEventMetadata(event);
        validateActor(event.actor());
        validateResource(event.resource());
        validateContext(event.context());
    }

    private void validateEventMetadata(AuditEvent event) {
        if (event.eventId() == null) {
            throw new AuditValidationException(
                    "Event ID must not be null"
            );
        }

        if (event.eventVersion() <= 0) {
            throw new AuditValidationException(
                    "Event version must be greater than zero"
            );
        }

        if (event.eventType() == null) {
            throw new AuditValidationException(
                    "Event type must not be null"
            );
        }

        if (event.timestamp() == null) {
            throw new AuditValidationException(
                    "Event timestamp must not be null"
            );
        }

        if (!StringUtils.hasText(event.action())) {
            throw new AuditValidationException(
                    "Event action must not be blank"
            );
        }
    }

    private void validateActor(Actor actor) {
        if (actor == null) {
            return;
        }

        if (actor.type() == null) {
            throw new AuditValidationException(
                    "Actor type must not be null"
            );
        }
    }

    private void validateResource(Resource resource) {
        if (resource == null) {
            return;
        }

        if (!StringUtils.hasText(resource.type())) {
            throw new AuditValidationException(
                    "Resource type must not be blank"
            );
        }

        if (!StringUtils.hasText(resource.id())) {
            throw new AuditValidationException(
                    "Resource ID must not be blank"
            );
        }
    }

    private void validateContext(AuditContext context) {
        if (context == null) {
            throw new AuditValidationException(
                    "Audit context must not be null"
            );
        }

        if (!StringUtils.hasText(context.applicationName())) {
            throw new AuditValidationException(
                    "Application name must not be blank"
            );
        }

        if (!StringUtils.hasText(context.serviceName())) {
            throw new AuditValidationException(
                    "Service name must not be blank"
            );
        }
    }
}