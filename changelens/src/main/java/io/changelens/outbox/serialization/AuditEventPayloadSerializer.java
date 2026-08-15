package io.changelens.outbox.serialization;

import io.changelens.core.domain.audit.AuditEvent;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

import java.util.Map;

@Component
public class AuditEventPayloadSerializer {

    private final JsonMapper jsonMapper;

    public AuditEventPayloadSerializer(JsonMapper jsonMapper) {
        this.jsonMapper = jsonMapper;
    }

    public Map<String, Object> serialize(AuditEvent event) {
        return jsonMapper.convertValue(event, Map.class);
    }

    public String serializeToString(AuditEvent event) {
        return jsonMapper.writeValueAsString(event);
    }
}
