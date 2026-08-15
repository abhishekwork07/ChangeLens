# Stage 4 — Recovery, Idempotency & DLQ

## Overview

Stage 4 introduces the reliability and recovery mechanisms for audit-event processing.

The primary goal of this stage is to ensure that audit events are processed **idempotently and safely under duplicate delivery, concurrent processing, processing failures, stale processing states, and malformed input**.

The implementation establishes a persistent processing state for each audit event and provides controlled recovery and DLQ routing.

---

## Objectives

Stage 4 addresses the following reliability requirements:

* Prevent duplicate audit-event processing.
* Prevent concurrent consumers from processing the same event.
* Track processing attempts.
* Recover events that remain stuck in `PROCESSING`.
* Retry failed events within the configured maximum attempt limit.
* Route events to the DLQ when maximum attempts are exhausted.
* Preserve malformed Kafka payloads in the DLQ.
* Ensure stale-event recovery is concurrency-safe.
* Maintain transactional state transitions.

---

## Processing State

Audit processing is tracked using the following states:

```text
                    ┌─────────────┐
                    │   STARTED   │
                    └──────┬──────┘
                           │
                           ▼
                    ┌─────────────┐
             ┌─────▶│  PROCESSING │
             │      └──────┬──────┘
             │             │
             │       success
             │             │
             │             ▼
             │      ┌─────────────┐
             │      │  PROCESSED  │
             │      └─────────────┘
             │
             │ failure
             │
             ▼
      ┌─────────────┐
      │   FAILED    │
      └──────┬──────┘
             │
             │ retry
             ▼
      ┌─────────────┐
      │  PROCESSING │
      └──────┬──────┘
             │
             │ max attempts
             ▼
      ┌─────────────────────┐
      │        DLQ          │
      └─────────────────────┘
```

Stale `PROCESSING` records can also be reclaimed and moved into another processing attempt.

---

## Idempotency

Each audit event is identified by its `eventId`.

The `audit_processing` table maintains the processing state:

```text
event_id
status
attempts
received_at
processed_at
last_error
created_at
updated_at
```

The processing claim is implemented using database-level conditional operations.

### Initial claim

A new event is inserted using:

```sql
ON CONFLICT (event_id) DO NOTHING
```

This prevents two consumers from successfully creating the same processing record.

### Duplicate processing

Already processed events return:

```text
ALREADY_PROCESSED
```

and are ignored.

Events currently being processed return:

```text
ALREADY_PROCESSING
```

and are not processed again.

---

## Concurrent Processing Protection

Concurrent processing of the same event is protected at the database level.

For a new event:

```text
Consumer A ──┐
             ├── tryStartProcessing()
Consumer B ──┘

             ↓

       One INSERT succeeds

             ↓

Consumer A → STARTED
Consumer B → concurrent claim resolution
```

Only one consumer is allowed to claim the event.

---

## Failure Recovery

When processing fails:

1. The processing service records the failure.
2. The event transitions to `FAILED`.
3. The exception is propagated back to Kafka.
4. Kafka can redeliver the event according to the configured retry policy.
5. The failed processing record is claimed again.
6. The attempt counter is incremented.
7. Processing resumes.

The retry operation is guarded by:

```sql
status = 'FAILED'
AND attempts < :maxAttempts
```

This ensures that the attempt limit is enforced at the database level.

---

## Maximum Retry Handling

Once the configured maximum number of attempts has been reached:

```text
MAX_ATTEMPTS_REACHED
```

is returned by the idempotency layer.

The event is then routed to the audit DLQ.

The DLQ record contains:

* `eventId`
* `status`
* `attempts`
* `payload`
* `errorMessage`
* `failedAt`

This provides enough information for subsequent investigation and recovery.

---

## Malformed Payload Handling

Malformed Kafka payloads cannot be deserialized into an `AuditEvent`.

These payloads are handled separately from normal event processing.

The original raw payload is preserved in the DLQ rather than attempting to construct an `AuditEvent`.

This ensures that malformed messages are not lost and can be investigated or replayed later.

---

## Stale Processing Recovery

A processing record can remain in `PROCESSING` if the consumer or application fails after claiming the event.

Stage 4 introduces stale-processing recovery.

An event is considered stale when:

```text
updatedAt < currentTime - processingTimeout
```

A stale record can be reclaimed using a conditional update:

```sql
UPDATE audit_processing
SET status = 'PROCESSING',
    attempts = attempts + 1,
    updated_at = :updatedAt
WHERE event_id = :eventId
  AND status = 'PROCESSING'
  AND updated_at < :staleThreshold
  AND attempts < :maxAttempts
```

The conditional update ensures that only one concurrent recovery operation can successfully reclaim the event.

---

## Concurrent Stale Recovery

Two consumers may detect the same stale event simultaneously.

The recovery operation therefore relies on an atomic database update rather than:

```text
read → check → update
```

Instead:

```text
Consumer A ──┐
             │
Consumer B ──┼── conditional UPDATE
             │
             ▼

        One update = 1
        Other update = 0
```

Expected result:

```text
Consumer A → RETRY_STARTED
Consumer B → ALREADY_PROCESSING
```

The processing attempt count is incremented only once.

---

# Stage 4 Test Coverage

The following integration scenarios have been implemented and verified.

| Scenario                                           | Result     |
| -------------------------------------------------- | ---------- |
| Successful audit-event consumption                 | ✅ Passed   |
| Duplicate event delivery                           | ✅ Passed   |
| Concurrent delivery of same event                  | ✅ Passed   |
| Maximum attempts exhausted                         | ✅ Passed   |
| Event routed to audit DLQ                          | ✅ Passed   |
| Malformed JSON payload routed to raw DLQ           | ✅ Passed   |
| Stale `PROCESSING` event recovery                  | ✅ Passed   |
| Concurrent stale-event recovery                    | ✅ Passed   |
| Application context loading                        | ✅ Passed   |
| Kafka failure → broker redelivery integration test | ⏸ Deferred |

### Deferred Kafka Retry Test

The `KafkaRetryIntegrationTest` has been temporarily excluded from the milestone because of an **Embedded Kafka 4.1.2 broker/checkpoint cleanup issue** occurring during broker shutdown.

The remaining Stage 4 reliability and recovery scenarios have been independently verified.

This test will be investigated separately without changing the currently verified recovery implementation.

---

# Key Components

Stage 4 introduced/updated the following components:

```text
AuditEventProcessor
        │
        ├── AuditEventDeserializer
        ├── AuditEventValidator
        ├── IdempotencyService
        ├── AuditProcessingService
        └── AuditDlqService
```

### IdempotencyService

Responsible for:

* Initial processing claims
* Duplicate detection
* Failed-event retry
* Stale-event recovery
* Concurrent claim resolution
* Maximum-attempt detection

### AuditDlqService

Responsible for:

* Moving failed audit events to DLQ
* Preserving raw malformed payloads
* Recording failure metadata

### AuditProcessingRepository

Provides atomic database operations for:

* Initial processing claim
* Successful completion
* Failure transition
* Failed-event retry
* Stale-event reclamation

---

# Reliability Guarantees

At the completion of Stage 4, the processing pipeline provides the following guarantees:

### Exactly one active processor

A given audit event cannot be actively claimed by multiple consumers through the idempotency layer.

### Duplicate-safe processing

Previously processed events are ignored.

### Attempt tracking

Every processing attempt is persisted.

### Controlled retry

Failed events can be retried while the configured attempt limit has not been reached.

### DLQ protection

Events exceeding the retry limit are routed to the DLQ.

### Malformed-message preservation

Malformed Kafka payloads are preserved as raw DLQ records.

### Stale recovery

Events left in `PROCESSING` can be safely reclaimed.

### Concurrent recovery protection

Only one consumer can successfully reclaim a stale event.

---

# Stage 4 Exit Criteria

Stage 4 is considered complete for the current milestone when:

* [x] Persistent processing state implemented
* [x] Idempotent event claiming implemented
* [x] Duplicate processing prevented
* [x] Concurrent processing prevented
* [x] Processing attempt tracking implemented
* [x] Failed-event recovery implemented
* [x] Maximum retry handling implemented
* [x] Audit DLQ implemented
* [x] Raw malformed-payload DLQ implemented
* [x] Stale processing recovery implemented
* [x] Concurrent stale recovery implemented
* [x] Integration tests implemented for recovery scenarios
* [x] Full application context test verified
* [ ] Embedded Kafka retry integration test — deferred

---