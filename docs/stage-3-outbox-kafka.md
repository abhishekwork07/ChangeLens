# ChangeLens — Stage 3: Outbox Publisher & Kafka

## Status

✅ Completed

## Objective

Implement reliable asynchronous publication of persisted outbox events to Kafka using an at-least-once delivery model.

## Achievements

### Outbox Claiming

Implemented transactional outbox event claiming:

```text
PENDING
   ↓
FOR UPDATE SKIP LOCKED
   ↓
PROCESSING
````

Features:

* Batch-based event claiming
* PostgreSQL row-level locking
* `SKIP LOCKED` for concurrent publishers
* Attempt tracking
* Safe concurrent processing across publisher instances

### Kafka Publisher

Implemented:

* `KafkaEventProducer`
* Kafka producer implementation
* Kafka acknowledgement handling
* Event ID as Kafka message key
* JSON payload publication
* Kafka failure handling

Publication flow:

```text
PROCESSING
     ↓
Publish to Kafka
     ↓
Kafka acknowledgement
```

### Outbox Status Management

Implemented transactional state transitions:

```text
PROCESSING → PUBLISHED
PROCESSING → FAILED
```

Successful publication records:

* `PUBLISHED` status
* `publishedAt`
* `updatedAt`

Failed publication records:

* `FAILED` status
* `lastError`
* `updatedAt`

### Processing Recovery

Implemented recovery for abandoned `PROCESSING` events.

```text
PROCESSING
     ↓
processing timeout exceeded
     ↓
PENDING
```

Recovery preserves the existing publication attempt count.

### Scheduler

Implemented scheduled execution for:

* Outbox event publication
* Stale `PROCESSING` event recovery

Configurable properties:

```yaml
changelens:
  outbox:
    publisher:
      interval:
      batch-size:

    recovery:
      interval:
      processing-timeout:
```

## Transaction Boundaries

Database transactions are intentionally separated from Kafka network calls.

```text
Claim Transaction
      ↓
PENDING → PROCESSING
      ↓
COMMIT
      ↓
Kafka Publication
      ↓
Status Transaction
      ↓
PUBLISHED / FAILED
```

Kafka publication does not hold a PostgreSQL transaction open.

## Delivery Semantics

The outbox publisher provides:

> **At-least-once delivery from PostgreSQL to Kafka.**

A publisher crash after successful Kafka publication but before the database status update can result in duplicate publication.

This is intentional.

Downstream Kafka consumers are expected to provide idempotent processing using `eventId`.

## Concurrency

Concurrent publishers are supported using:

```sql
FOR UPDATE SKIP LOCKED
```

This prevents multiple publisher workers from claiming the same pending outbox records during concurrent processing.

## Testing

Stage 3 was verified with:

* Outbox claiming integration tests
* Kafka publication tests
* Kafka failure tests
* Processing recovery tests
* Concurrent publisher tests
* Crash/recovery scenario tests
* Scheduler tests
* Complete Maven test suite
* Maven `verify`

Concurrency tests were executed repeatedly to validate the `SKIP LOCKED` behavior.

## Stage Outcome

ChangeLens can now reliably move persisted audit events from the transactional outbox to Kafka without coupling the database transaction to the Kafka network call.

The complete pipeline is now:

```text
AuditEvent
    ↓
Validate
    ↓
Persist audit_event
    ↓
Persist audit_change
    ↓
Persist outbox_event
    ↓
PENDING
    ↓
Outbox Publisher
    ↓
PROCESSING
    ↓
Kafka
    ├── Success → PUBLISHED
    └── Failure → FAILED
```

Stale publisher states can be recovered:

```text
PROCESSING
    ↓
timeout
    ↓
PENDING
```