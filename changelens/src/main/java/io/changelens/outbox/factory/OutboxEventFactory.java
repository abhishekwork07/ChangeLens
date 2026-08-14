package io.changelens.outbox.factory;

import io.changelens.core.domain.audit.AuditEvent;
import io.changelens.core.enums.AggregateType;
import io.changelens.core.enums.OutboxEventStatusType;
import io.changelens.outbox.entity.OutboxEventEntity;
import io.changelens.outbox.serialization.AuditEventPayloadSerializer;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class OutboxEventFactory {

    private final AuditEventPayloadSerializer payloadSerializer;

    public OutboxEventEntity create(AuditEvent event) {

        AggregateType aggregateType = event.resource() != null
                ? AggregateType.RESOURCE
                : AggregateType.UNKNOWN;

        String aggregateId = event.resource() != null
                ? event.resource().id()
                : event.eventId().toString();

        return OutboxEventEntity.builder()
                .eventId(event.eventId())
                .aggregateType(aggregateType.name())
                .aggregateId(aggregateId)
                .eventType(event.eventType())
                .payload(payloadSerializer.serialize(event))
                .status(OutboxEventStatusType.PENDING)
                .attempts(0)
                .build();
    }
}
