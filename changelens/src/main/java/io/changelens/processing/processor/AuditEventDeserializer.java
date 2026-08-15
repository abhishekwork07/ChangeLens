package io.changelens.processing.processor;

import io.changelens.core.domain.audit.AuditEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

@Component
@RequiredArgsConstructor
public class AuditEventDeserializer {

    private final JsonMapper jsonMapper;

    public AuditEvent deserialize(String payload) {
        try {
            return jsonMapper.readValue(payload, AuditEvent.class);
        } catch (JacksonException e) {
            throw new AuditDeserializationException("Failed to deserialize audit event", e);
        }
    }
}
