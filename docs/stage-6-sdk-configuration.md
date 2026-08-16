# ChangeLens SDK — Stage 6 Milestone

## Overview

Stage 6 completes the Kafka and Outbox infrastructure for the ChangeLens SDK and integrates the infrastructure through Spring Boot auto-configuration.

The stage was verified through the application's SDK configuration and the existing integration test suite. The final `mvn clean test` execution passed successfully.

## Stage 6 Scope

- Outbox event publishing
- Scheduled Outbox polling and publishing
- Kafka producer and consumer configuration
- Kafka retry and error handling
- Audit event consumption and processing
- Idempotency integration
- Redis processed-event cache
- Redis fallback to `NoOpProcessedEventCache`
- SDK auto-configuration for optional infrastructure
- JPA integration for entity-level auditing

## Architecture

```text
Audit capture
    |
    v
AuditEventFactory
    |
    v
AuditEventPublisher
    |
    v
OutboxEventWriter
    |
    v
outbox_event
    |
    v
OutboxPublisher
    |
    v
Kafka
    |
    v
AuditEventConsumer
    |
    v
AuditEventProcessor
    |
    v
IdempotencyService
    |
    +----> Redis fast-path
    |
    v
AuditProcessingService
    |
    +----> PROCESSED
    +----> FAILED -> Kafka retry
    +----> DLQ after maximum attempts
```

## Outbox Publishing

The Outbox pattern decouples audit event creation from Kafka publishing.

Flow:

1. An audit event is written to `outbox_event`.
2. `OutboxPublisher` claims pending events.
3. Events are published to Kafka.
4. Successful events are marked `PUBLISHED`.
5. Failed events are marked `FAILED`.
6. Stale `PROCESSING` records can be recovered.

Main components:

- `OutboxEventEntity`
- `OutboxEventRepository`
- `OutboxEventWriter`
- `OutboxClaimService`
- `OutboxStatusService`
- `OutboxPublisher`
- `OutboxPublishingScheduler`

Pending records use database locking with:

```sql
FOR UPDATE SKIP LOCKED
```

to support concurrent publishers without processing the same rows.

## Kafka Infrastructure

### Producer

```text
ProducerFactory<String, Object>
KafkaTemplate<String, Object>
JacksonJsonSerializer
```

### Consumer

The consumer receives the Kafka value as a JSON `String`:

```text
Kafka
  |
  v
AuditEventConsumer.consume(String payload)
  |
  v
AuditEventProcessor
  |
  v
AuditEventDeserializer
  |
  v
AuditEvent
```

This keeps Kafka transport deserialization separate from the ChangeLens audit-event deserialization and validation pipeline.

## Kafka Retry

The retry flow verified by integration testing is:

```text
Kafka delivery
    |
    v
First processing attempt
    |
    v
Processing failure
    |
    v
audit_processing = FAILED
    |
    v
Kafka retry
    |
    v
Second processing attempt
    |
    v
audit_processing = PROCESSED
```

The integration test verifies that:

- the first processing attempt fails;
- the failure is recorded;
- Kafka redelivers the message;
- the second attempt succeeds;
- the processing record reaches `PROCESSED`;
- the attempt count reaches `2`.

## Idempotency

`IdempotencyService` controls processing claims and duplicate protection.

The main states are:

```text
No record      -> STARTED
PROCESSING     -> ALREADY_PROCESSING
FAILED         -> RETRY_STARTED
PROCESSED      -> ALREADY_PROCESSED
```

Stale processing records can be reclaimed using the configured processing timeout, subject to the maximum retry limit.

## Redis Processed Event Cache

Redis provides a fast path for already processed events.

Keys use:

```text
changelens:processed:<event-id>
```

Example configuration:

```yaml
changelens:
  redis:
    processed-event-ttl: 24h
```

The lookup flow is:

```text
IdempotencyService
    |
    v
Redis cache lookup
    |
    +-- hit  --> ALREADY_PROCESSED
    |
    +-- miss --> database idempotency check
```

Redis is an optimization; the database remains the source of truth.

### Redis Fallback

When Redis-backed caching is not available, the SDK provides:

```text
NoOpProcessedEventCache
```

using:

```java
@ConditionalOnMissingBean(ProcessedEventCache.class)
```

This also allows consuming applications to provide their own `ProcessedEventCache` implementation.

## SDK Auto-Configuration

Stage 6 completes the infrastructure auto-configuration model:

```text
ChangeLensAutoConfiguration
ChangeLensJpaAutoConfiguration
ChangeLensRedisAutoConfiguration
ChangeLensOutboxAutoConfiguration
ChangeLensKafkaAutoConfiguration
```

Infrastructure is conditionally activated based on the required dependencies and configuration.

Central SDK properties are exposed through:

```java
@ConfigurationProperties(prefix = "changelens")
public class ChangeLensProperties
```

## JPA Entity Auditing

Entity-level auditing uses Hibernate `POST_UPDATE` events.

Core flow:

```text
Hibernate POST_UPDATE
    |
    v
AuditPostUpdateEventListener
    |
    v
AuditEntityMetadataResolver
    |
    v
Field changes
    |
    v
AuditEventFactory
    |
    v
AuditEventPublisher
```

`AuditHibernateEventListenerRegistrar` registers the listener with Hibernate.

Only auditable entities and configured fields are considered, and an audit event is generated when relevant field changes are detected.

## Verification

Stage 6 was verified through the SDK's actual Spring Boot configuration and integration tests.

### Redis

Verified:

- processed event caching;
- cache lookup;
- eviction;
- TTL;
- database fallback on cache miss;
- Redis-backed implementation selection;
- no-op fallback when Redis is unavailable.

### Kafka

Verified:

- producer configuration;
- consumer configuration;
- audit event consumption;
- retry behavior;
- failed processing followed by successful retry;
- processing attempt count;
- final `PROCESSED` state.

### Outbox

Verified:

- pending outbox event publishing;
- publishing status updates;
- integration with Kafka publishing.

### Idempotency

Verified:

- initial event claim;
- duplicate processing protection;
- failed-event retry;
- maximum-attempt handling;
- processing state transitions.

### Full Test Suite

Final verification:

```bash
mvn clean test
```

Result:

```text
BUILD SUCCESS
```

## Stage 6 Milestone

The completed infrastructure path is:

```text
Audit capture
    |
    v
AuditEvent
    |
    v
Outbox
    |
    v
Kafka
    |
    v
AuditEventConsumer
    |
    v
AuditEventProcessor
    |
    v
Idempotency
    |
    v
Business processing
    |
    +--> PROCESSED
    +--> FAILED -> Retry
    +--> MAX_ATTEMPTS_REACHED -> DLQ
```

Stage 6 is considered a completed implementation milestone because the functionality was verified through the SDK's actual configuration path rather than only through isolated component tests.
