package io.changelens.outbox.publisher;

import io.changelens.core.enums.OutboxEventStatusType;
import io.changelens.outbox.entity.OutboxEventEntity;
import io.changelens.outbox.repository.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
@RequiredArgsConstructor
public class OutboxClaimService {

    private final OutboxEventRepository outboxEventRepository;

    @Transactional
    public List<OutboxEventEntity> claimPendingEvents(int batchSize) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("Batch size must be greater than zero");
        }

        List<OutboxEventEntity> events =
                outboxEventRepository.findPendingEventsForPublishing(batchSize);

        for (OutboxEventEntity event : events) {
            event.setStatus(OutboxEventStatusType.PROCESSING);
            event.setAttempts(event.getAttempts() + 1);
        }

        return events;
    }


}
