package io.changelens.processing.consumer;

import io.changelens.processing.processor.AuditEventProcessor;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class AuditEventConsumer {

    private final AuditEventProcessor processor;

    @KafkaListener(
            topics = "${changelens.kafka.topics.audit-events}",
            groupId = "${changelens.kafka.consumer.group-id}"
    )
    public void consume(String payload) {
        processor.process(payload);
    }
}
