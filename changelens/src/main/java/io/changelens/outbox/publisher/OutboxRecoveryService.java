package io.changelens.outbox.publisher;

import io.changelens.outbox.repository.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;

@Service
@RequiredArgsConstructor
public class OutboxRecoveryService {

    private final OutboxEventRepository outboxEventRepository;

    @Transactional
    public int recoverStaleEvents(Duration processingTimeout) {
        Instant now = Instant.now();
        Instant threshold = now.minus(processingTimeout);

        return outboxEventRepository
                .recoverStaleProcessingEvents(threshold, now);
    }
}
