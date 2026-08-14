package io.changelens.storage.entity;

import io.changelens.core.domain.actor.ActorType;
import io.changelens.core.domain.audit.AuditEventType;
import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;
import org.springframework.data.annotation.CreatedDate;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@Builder
@Entity
@Table(name = "audit_event")
public class AuditEventEntity {

    @Id
    private UUID eventId;

    @Column(nullable = false)
    private int eventVersion;

    @Column(length = 64)
    private String tenantId;

    @Column(nullable = false)
    @Enumerated(EnumType.STRING)
    private AuditEventType eventType;

    @Column(name = "event_timestamp", nullable = false)
    private Instant eventTimestamp;

    @Column(nullable = false, length = 128)
    private String action;

    @Column(nullable = false)
    @Enumerated(EnumType.STRING)
    private ActorType actorType = ActorType.UNKNOWN;

    @Column(length = 64)
    private String actorId;

    @Column(length = 128)
    private String actorName;

    @Column(name = "resource_type", length = 128)
    private String resourceType;

    @Column(name = "resource_id", length = 128)
    private String resourceId;

    @Column(name = "resource_name", length = 256)
    private String resourceName;

    @Column(nullable = false, length = 128)
    private String applicationName;

    @Column(length = 64)
    private String applicationVersion;

    @Column(nullable = false, length = 128)
    private String serviceName;

    @Column(length = 32)
    private String environment;

    @Column(length = 64)
    private String requestId;

    @Column(length = 64)
    private String correlationId;

    private String traceId;

    @Column(length = 64)
    private String sourceIp;

    @Column(length = 512)
    private String userAgent;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "before_state", columnDefinition = "jsonb")
    private Object beforeState;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "after_state", columnDefinition = "jsonb")
    private Object afterState;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "extensions", columnDefinition = "jsonb")
    private Map<String, Object> extensions = Map.of();

    @CreatedDate
    @Column(nullable = false, updatable = false)
    private Instant createdAt;

}
