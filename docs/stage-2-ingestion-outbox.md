# ChangeLens — Stage 2: Audit Ingestion & Transactional Outbox

## Status

✅ Completed

## Objective

Establish the first transactional audit ingestion workflow and guarantee that audit persistence and event publication intent are committed atomically.

## Achievements

### Audit Validation

Implemented:

- `AuditEventValidator`
- `AuditValidationException`

Validates core event metadata, actor, resource, and audit context before persistence.

### Audit Ingestion

Implemented `AuditIngestionService` responsible for:

- Validating `AuditEvent`
- Mapping domain event to persistence entity
- Persisting `AuditEventEntity`
- Mapping and persisting `AuditChangeEntity` records

### Transactional Outbox

Implemented:

- `AuditEventPayloadSerializer`
- `OutboxEventFactory`
- `OutboxEventRepository`

Outbox events are created with:

- Event identity
- Aggregate information
- Event type
- Serialized event payload
- `PENDING` status
- Initial retry attempt count

## Transaction Boundary

All audit persistence and outbox creation execute within a single transaction:

```text
AuditEvent
    ↓
Validate
    ↓
Persist audit_event
    ↓
Persist audit_change[]
    ↓
Create outbox_event
    ↓
COMMIT
```
If any persistence operation fails:
```text
Any Failure
    ↓
Transaction Rollback
    ↓
No partial audit state
```
## Reliability Guarantee

ChangeLens guarantees that:

- `An audit event and its corresponding publication intent are committed atomically`

The database transaction does not include Kafka. Kafka publication will be handled asynchronously from the persisted outbox.
```text
PostgreSQL Transaction
        │
        ├── audit_event
        ├── audit_change
        └── outbox_event
                 │
                 ▼
              Kafka
```
## Stage Outcome

ChangeLens can now validate and persist audit events, field-level changes, and their transactional publication intent as one atomic database operation.


