# Stage 5 – Redis-Backed Idempotency & SDK Readiness

## Milestone Objective

Stage 5 introduces Redis as a performance optimization for ChangeLens idempotency processing while keeping PostgreSQL as the authoritative source of truth.

The goal is to reduce database lookups for already-processed audit events without changing the business logic required by integrating products.

## Completed

### 1. Redis Infrastructure

Added Spring Boot Redis support:

```xml
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-data-redis</artifactId>
</dependency>
```

Redis configuration:

```yaml
spring:
  data:
    redis:
      host: localhost
      port: 6379
      timeout: 2s
```

Docker Compose Redis service uses `redis:8-alpine`, port `6379`, AOF persistence, and a dedicated Docker volume.

---

### 2. Processed Event Cache

Introduced the `ProcessedEventCache` abstraction with a Redis implementation:

```text
ProcessedEventCache
        |
        v
RedisProcessedEventCache
        |
        v
StringRedisTemplate
```

Redis keys use:

```text
changelens:processed:<eventId>
```

Processed-event entries have a configurable TTL:

```yaml
changelens:
  redis:
    processed-event-ttl: 24h
```

Supported operations:

- `contains(eventId)`
- `put(eventId)`
- `evict(eventId)`

---

### 3. Redis-Optimized Idempotency

`IdempotencyService` now performs a fast Redis lookup before querying PostgreSQL.

```text
Audit Event
    |
    v
IdempotencyService
    |
    +---- Redis HIT ------> ALREADY_PROCESSED
    |
    +---- Redis MISS
              |
              v
        PostgreSQL
              |
       +------+------+
       |             |
   PROCESSED      PROCESSING/
                  FAILED/new
```

PostgreSQL remains the source of truth.

Redis is only an optimization and is not required for correctness.

---

### 4. Redis Failure Resilience

Redis availability must not affect audit-processing correctness.

```text
Redis failure
     |
     v
Cache operation fails safely
     |
     v
Fallback to PostgreSQL
```

The cache implementation handles Redis connection failures and allows processing to continue using the database.

---

### 5. Integration Test Coverage

Stage 5 tests verify:

- Redis cache hit/miss behavior
- Processed-event insertion
- Cache eviction
- TTL configuration
- Redis unavailable fallback
- Redis miss followed by PostgreSQL lookup
- Starting processing when no processing record exists
- Returning `ALREADY_PROCESSED` from the authoritative database state
- Redis cache population after discovering a processed event from PostgreSQL
- Database foreign-key integrity between `audit_event` and `audit_processing`

---

### 6. Verification

The complete test suite was successfully verified with:

```bash
mvn clean test
```

Result:

```text
BUILD SUCCESS
```

This confirms the Stage 5 Redis changes do not break the previously completed functionality.

---

## Architecture After Stage 5

```text
                    +------------------+
                    |   Audit Event    |
                    +--------+---------+
                             |
                             v
                    +-------------------+
                    | AuditEventProcessor|
                    +--------+----------+
                             |
                             v
                    +-------------------+
                    | IdempotencyService |
                    +--------+----------+
                             |
                    +--------+--------+
                    |                 |
                    v                 v
              +-----------+     +-----------+
              |   Redis   |     | PostgreSQL|
              |  Cache    |     |  Source   |
              +-----------+     | of Truth  |
                                +-----------+
```

## MVP Design Principle

The integration contract for consuming products remains minimal.

Consuming products should not need to manage Redis, idempotency state, retry state, cache invalidation, or audit-processing persistence directly.

These concerns remain inside the ChangeLens SDK/infrastructure.

The intended integration model remains annotation-driven or equivalent SDK-level integration without requiring changes to the consuming product's business logic.

---

## Deferred Item

### Kafka Retry Integration Test

The Kafka retry integration test is currently deferred.

The implementation and existing retry behavior remain part of the project, but the integration test encountered Embedded Kafka 4.1.2 temporary-directory/checkpoint cleanup issues during the test lifecycle.

This is intentionally deferred so the Redis milestone can be committed independently of that test-infrastructure issue.

---

## Stage 5 Outcome

Stage 5 establishes Redis-backed idempotency as an internal optimization while preserving PostgreSQL-backed correctness.

The key guarantee is:

```text
Redis available
    -> faster idempotency checks

Redis unavailable
    -> PostgreSQL continues to provide correctness
```

ChangeLens is now better positioned for the remaining MVP work around SDK packaging, reusable integration APIs, auto-configuration, and adoption by other Spring Boot products.