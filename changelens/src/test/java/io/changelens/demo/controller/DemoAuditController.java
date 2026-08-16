package io.changelens.demo.controller;

import io.changelens.outbox.entity.OutboxEventEntity;
import io.changelens.outbox.repository.OutboxEventRepository;
import io.changelens.storage.entity.AuditEventEntity;
import io.changelens.storage.entity.AuditProcessingEntity;
import io.changelens.storage.repository.AuditEventRepository;
import io.changelens.storage.repository.AuditProcessingRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/demo/audit")
@RequiredArgsConstructor
public class DemoAuditController {

    private final AuditEventRepository auditEventRepository;
    private final AuditProcessingRepository auditProcessingRepository;
    private final OutboxEventRepository outboxEventRepository;

    @GetMapping("/events")
    public List<AuditEventEntity> events() {
        return auditEventRepository.findAll();
    }

    @GetMapping("/events/{eventId}")
    public AuditEventEntity event(
            @PathVariable UUID eventId) {

        return auditEventRepository
                .findByEventId(eventId)
                .orElseThrow(() ->
                        new IllegalArgumentException(
                                "Audit event not found: " + eventId
                        ));
    }

    @GetMapping("/processing/{eventId}")
    public AuditProcessingEntity processing(
            @PathVariable UUID eventId) {

        return auditProcessingRepository
                .findById(eventId)
                .orElseThrow(() ->
                        new IllegalArgumentException(
                                "Processing record not found: " + eventId
                        ));
    }

    @GetMapping("/outbox")
    public List<OutboxEventEntity> outbox() {
        return outboxEventRepository.findAll();
    }
}