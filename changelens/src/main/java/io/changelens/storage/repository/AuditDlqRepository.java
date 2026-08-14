package io.changelens.storage.repository;

import io.changelens.storage.entity.AuditDlqEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface AuditDlqRepository extends JpaRepository<AuditDlqEntity, UUID> {

}
