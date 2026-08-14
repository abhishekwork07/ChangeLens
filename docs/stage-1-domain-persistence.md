# ChangeLens — Stage 1: Domain, Event & Persistence

## Status

✅ Completed

## Objective

Establish the core domain model and persistence foundation required to represent, store, and track ChangeLens audit events.

## Achievements

### Domain / Event Model

Created the framework-independent audit domain model:

- `AuditEvent`
- `Actor`
- `Resource`
- `AuditChange`
- `AuditContext`
- Supporting event/change enums
- `ChangeSet`

The domain model is independent of JPA, PostgreSQL, Kafka, and other infrastructure concerns.

### JPA Persistence Model

Created persistence entities for:

- `AuditEventEntity`
- `AuditChangeEntity`
- `AuditProcessingEntity`
- `AuditDlqEntity`
- `OutboxEventEntity`

The persistence model represents:

```text
Audit Event
    ├── Changes
    ├── Processing State
    └── DLQ State

Audit Event
    └── Outbox Event
```

## Stage Outcome

ChangeLens now has a complete domain and persistence foundation capable of representing audit events, field-level changes, processing state, dead-letter state, and transactional outbox records.