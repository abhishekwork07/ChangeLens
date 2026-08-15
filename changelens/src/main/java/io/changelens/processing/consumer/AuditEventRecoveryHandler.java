package io.changelens.processing.consumer;

import io.changelens.core.domain.audit.AuditEvent;
import io.changelens.processing.dlq.AuditDlqService;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

@Component
@RequiredArgsConstructor
public class AuditEventRecoveryHandler implements ConsumerRecordRecoverer {

    private final AuditDlqService dlqService;
    private final JsonMapper jsonMapper;

    @Override
    public void accept(ConsumerRecord<?, ?> record, Exception exception) {

        String payload = record.value() != null
                ? record.value().toString()
                : null;

        if (payload == null || payload.isBlank()) {
            dlqService.moveRawPayloadToDlq(payload, exception.getMessage());
            return;
        }

        try {
            AuditEvent event = jsonMapper.readValue(payload, AuditEvent.class);
            dlqService.moveToDlq(event.eventId(), payload, exception.getMessage());
        } catch (JacksonException e) {
            dlqService.moveRawPayloadToDlq(payload, exception.getMessage());
        }
    }
}
