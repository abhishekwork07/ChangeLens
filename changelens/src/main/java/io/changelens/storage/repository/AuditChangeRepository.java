package io.changelens.storage.repository;

import io.changelens.storage.entity.AuditChangeEntity;
import org.springframework.data.jpa.repository.JpaRepository;

public interface AuditChangeRepository extends JpaRepository<AuditChangeEntity, Long> {

}
