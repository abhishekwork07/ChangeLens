package io.changelens.storage.entity;

import io.changelens.core.enums.AuditStatusType;
import jakarta.persistence.*;
import lombok.*;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.LastModifiedDate;
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
@Table(name = "audit_processing")
public class AuditProcessingEntity {

    @Id
    private UUID eventId;

    @Column(nullable = false, length = 32)
    @Enumerated(EnumType.STRING)
    private AuditStatusType status;

    @Column(nullable = false)
    private int attempts = 0;

    @Column(nullable = false, updatable = false)
    private Instant receivedAt;

    private Instant processedAt;

    @Column(length = 2048)
    private String lastError;

    @CreatedDate
    @Column(nullable = false, updatable = false)
    private Instant createdAt;

    @LastModifiedDate
    @Column(nullable = false)
    private Instant updatedAt;
}
