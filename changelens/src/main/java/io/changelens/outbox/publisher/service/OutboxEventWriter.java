package io.changelens.outbox.publisher.service;

import io.changelens.core.domain.audit.AuditEvent;
import io.changelens.core.enums.OutboxEventStatusType;
import io.changelens.outbox.entity.OutboxEventEntity;
import io.changelens.outbox.repository.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;
import java.util.Map;

@RequiredArgsConstructor
public class OutboxEventWriter {

    private final OutboxEventRepository repository;
    private final JsonMapper jsonMapper;

    public void write(AuditEvent event) {

        Map<String, Object> payload =
                jsonMapper.convertValue(
                        event, new TypeReference<>() {
                        });

        Instant now = Instant.now();

        OutboxEventEntity entity =
                OutboxEventEntity.builder()
                        .eventId(event.eventId())
                        .aggregateType("AUDIT_EVENT")
                        .aggregateId(event.eventId().toString())
                        .eventType(event.eventType())
                        .payload(payload)
                        .status(OutboxEventStatusType.PENDING)
                        .createdAt(now)
                        .publishedAt(null)
                        .attempts(0)
                        .lastError(null)
                        .updatedAt(now)
                        .build();

        repository.save(entity);
    }
}