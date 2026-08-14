package io.changelens.storage.mapper;

import io.changelens.core.context.AuditContext;
import io.changelens.core.domain.actor.Actor;
import io.changelens.core.domain.actor.ActorType;
import io.changelens.core.domain.audit.AuditEvent;
import io.changelens.core.domain.change.ChangeSet;
import io.changelens.core.domain.resource.Resource;
import io.changelens.storage.entity.AuditEventEntity;
import org.springframework.stereotype.Component;

@Component
public class AuditEventMapper {

    public AuditEventEntity toEntity(AuditEvent event) {
        Actor actor = event.actor();
        Resource resource = event.resource();
        AuditContext context = event.context();

        return AuditEventEntity.builder()
                .eventId(event.eventId())
                .eventVersion(event.eventVersion())
                .tenantId(event.tenantId())
                .eventType(event.eventType())
                .eventTimestamp(event.timestamp())
                .action(event.action())

                // Actor
                .actorType(actor != null ? actor.type() : ActorType.UNKNOWN)
                .actorId(actor != null ? actor.id() : null)
                .actorName(actor != null ? actor.name() : null)

                // Resource
                .resourceType(resource != null ? resource.type() : null)
                .resourceId(resource != null ? resource.id() : null)
                .resourceName(resource != null ? resource.name() : null)

                // Context
                .applicationName(context != null ? context.applicationName() : null)
                .applicationVersion(context != null ? context.applicationVersion() : null)
                .serviceName(context != null ? context.serviceName() : null)
                .environment(context != null ? context.environment() : null)
                .requestId(context != null ? context.requestId() : null)
                .correlationId(context != null ? context.correlationId() : null)
                .traceId(context != null ? context.traceId() : null)
                .sourceIp(context != null ? context.sourceIp() : null)
                .userAgent(context != null ? context.userAgent() : null)

                .beforeState(event.beforeState())
                .afterState(event.afterState())
                .extensions(event.extensions())
                .build();
    }

    public AuditEvent toDomain(AuditEventEntity entity, ChangeSet changeSet) {
        Actor actor = null;
        if (entity.getActorType() != null
                || entity.getActorId() != null
                || entity.getActorName() != null) {
            actor = new Actor(
                    entity.getActorType(),
                    entity.getActorId(),
                    entity.getActorName()
            );
        }

        Resource resource = null;
        if (entity.getResourceType() != null
                || entity.getResourceId() != null
                || entity.getResourceName() != null) {
            resource = new Resource(
                    entity.getResourceType(),
                    entity.getResourceId(),
                    entity.getResourceName()
            );
        }

        AuditContext context = null;
        if (entity.getApplicationName() != null
                || entity.getApplicationVersion() != null
                || entity.getServiceName() != null
                || entity.getEnvironment() != null
                || entity.getRequestId() != null
                || entity.getCorrelationId() != null
                || entity.getTraceId() != null
                || entity.getSourceIp() != null
                || entity.getUserAgent() != null) {
            context = new AuditContext(
                    entity.getApplicationName(),
                    entity.getApplicationVersion(),
                    entity.getServiceName(),
                    entity.getEnvironment(),
                    entity.getRequestId(),
                    entity.getCorrelationId(),
                    entity.getTraceId(),
                    entity.getSourceIp(),
                    entity.getUserAgent()
            );
        }

        return new AuditEvent(
                entity.getEventId(),
                entity.getEventVersion(),
                entity.getTenantId(),
                entity.getEventType(),
                entity.getEventTimestamp(),
                entity.getAction(),
                actor,
                resource,
                changeSet,
                entity.getBeforeState(),
                entity.getAfterState(),
                context,
                entity.getExtensions()
        );
    }

}
