package io.changelens.storage.entity;

import io.changelens.core.enums.DlqStatusType;
import jakarta.persistence.*;
import lombok.*;
import org.springframework.data.jpa.domain.support.AuditingEntityListener;

import java.time.Instant;
import java.util.UUID;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@Builder
@Entity
@EntityListeners(AuditingEntityListener.class)
@Table(name = "audit_dlq")
public class AuditDlqEntity {

    @Id
    private UUID eventId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 32)
    private DlqStatusType status;

    @Column(nullable = false)
    private int attempts;

    @Column(name = "payload", columnDefinition = "TEXT", nullable = false)
    private String payload;

    @Column(nullable = false, length = 2048)
    private String errorMessage;

    @Column(nullable = false)
    private Instant failedAt;

    private Instant resolvedAt;
}
