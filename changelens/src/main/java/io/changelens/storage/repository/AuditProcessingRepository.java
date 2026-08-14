package io.changelens.storage.repository;

import io.changelens.storage.entity.AuditProcessingEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface AuditProcessingRepository extends JpaRepository<AuditProcessingEntity, UUID> {

}
