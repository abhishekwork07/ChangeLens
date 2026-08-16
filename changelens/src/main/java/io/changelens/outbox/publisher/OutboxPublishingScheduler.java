package io.changelens.outbox.publisher;

import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;

@RequiredArgsConstructor
public class OutboxPublishingScheduler {

    private final OutboxPublisher outboxPublisher;
    private final int batchSize;

    @Scheduled(
            fixedDelayString = "${changelens.outbox.publisher.fixed-delay:5000}"
    )
    public void publish() {
        outboxPublisher.publish(batchSize);
    }
}