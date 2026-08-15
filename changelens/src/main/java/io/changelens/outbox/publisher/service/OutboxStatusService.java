package io.changelens.outbox.publisher.service;

import io.changelens.outbox.publisher.OutboxStateTransitionException;
import io.changelens.outbox.repository.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class OutboxStatusService {

    private final OutboxEventRepository outboxEventRepository;

    @Transactional
    public void markPublished(UUID eventId, Instant publishedAt) {
        int updated = outboxEventRepository
                .updateEventAsPublished(eventId, publishedAt, Instant.now());

        if (updated != 1) {
            throw new OutboxStateTransitionException(
                    "Unable to mark outbox event as PUBLISHED: " + eventId);
        }
    }

    @Transactional
    public void markFailed(UUID eventId, String error) {
        int updated = outboxEventRepository
                .updateEventAsFailed(eventId, error, Instant.now());

        if (updated != 1) {
            throw new OutboxStateTransitionException(
                    "Unable to mark outbox event as FAILED: " + eventId);
        }
    }
}
