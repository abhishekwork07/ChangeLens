package io.changelens.storage.repository;

import io.changelens.storage.entity.AuditEventEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;
import java.util.UUID;

public interface AuditEventRepository extends JpaRepository<AuditEventEntity, UUID> {

    Optional<AuditEventEntity> findByEventId(UUID eventId);
}
