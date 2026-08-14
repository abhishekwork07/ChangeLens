package io.changelens.outbox.serialization;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.changelens.core.domain.audit.AuditEvent;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class AuditEventPayloadSerializer {

    private final ObjectMapper objectMapper;

    public AuditEventPayloadSerializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public Map<String, Object> serialize(AuditEvent event) {
        return objectMapper.convertValue(
                event,
                new TypeReference<>() {}
        );
    }
}
