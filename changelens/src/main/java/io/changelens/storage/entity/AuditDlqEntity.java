package io.changelens.storage.entity;

import io.changelens.core.enums.DlqStatusType;
import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@Builder
@Entity
@Table(name = "audit_dlq")
public class AuditDlqEntity {

    @Id
    private UUID eventId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 32)
    private DlqStatusType status;

    @Column(nullable = false)
    private int attempts;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "payload", columnDefinition = "jsonb", nullable = false)
    private Map<String, Object> payload;

    @Column(nullable = false, length = 2048)
    private String errorMessage;

    @Column(nullable = false)
    private Instant failedAt;

    private Instant resolvedAt;
}
