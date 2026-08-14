package io.changelens.core.application;

import io.changelens.core.domain.audit.AuditEvent;
import io.changelens.core.domain.change.AuditChange;
import io.changelens.core.validation.AuditEventValidator;
import io.changelens.outbox.entity.OutboxEventEntity;
import io.changelens.outbox.factory.OutboxEventFactory;
import io.changelens.outbox.repository.OutboxEventRepository;
import io.changelens.storage.entity.AuditChangeEntity;
import io.changelens.storage.entity.AuditEventEntity;
import io.changelens.storage.mapper.AuditChangeMapper;
import io.changelens.storage.mapper.AuditEventMapper;
import io.changelens.storage.repository.AuditChangeRepository;
import io.changelens.storage.repository.AuditEventRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;

@Service
@RequiredArgsConstructor
public class AuditIngestionService {

    private final AuditEventValidator validator;
    private final AuditEventMapper auditEventMapper;
    private final AuditChangeMapper auditChangeMapper;
    private final AuditEventRepository auditEventRepository;
    private final AuditChangeRepository auditChangeRepository;
    private final OutboxEventFactory outboxEventFactory;
    private final OutboxEventRepository outboxEventRepository;

    @Transactional
    public void ingest(AuditEvent event) {
        List<AuditChangeEntity> auditChanges = new ArrayList<>();

        validator.validate(event);

        AuditEventEntity eventEntity = auditEventMapper.toEntity(event);
        auditEventRepository.save(eventEntity);

        for (AuditChange change : event.changeSet().changes()) {
            AuditChangeEntity changeEntity =
                    auditChangeMapper.toEntity(change, eventEntity);
            auditChanges.add(changeEntity);
        }

        if (!auditChanges.isEmpty()) {
            auditChangeRepository.saveAll(auditChanges);
        }

        OutboxEventEntity outboxEvent = outboxEventFactory.create(event);
        outboxEventRepository.save(outboxEvent);
    }
}
