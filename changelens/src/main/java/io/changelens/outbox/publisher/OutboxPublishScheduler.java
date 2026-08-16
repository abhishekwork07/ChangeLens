package io.changelens.outbox.publisher;

import io.changelens.outbox.publisher.service.OutboxRecoveryService;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;

import java.time.Duration;

@RequiredArgsConstructor
public class OutboxPublishScheduler {

    private final OutboxPublisher outboxPublisher;
    private final OutboxRecoveryService recoveryService;

    @Value("${changelens.outbox.publisher.batch-size:100}")
    private int batchSize;

    @Value("${changelens.outbox.recovery.processing-timeout:5m}")
    private Duration processingTimeout;

    @Scheduled(
            fixedDelayString = "${changelens.outbox.publisher.interval:1000}"
    )
    public void publish() {
        outboxPublisher.publish(batchSize);
    }

    @Scheduled(
            fixedDelayString = "${changelens.outbox.recovery.interval:30000}"
    )
    public void recover() {
        recoveryService.recoverStaleEvents(processingTimeout);
    }
}
