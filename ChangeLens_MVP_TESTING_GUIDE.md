# ChangeLens MVP --- SDK & Testing Guide

This guide explains the ChangeLens SDK integration and how to
demonstrate the three MVP capabilities:

1.  Method-level auditing
2.  Class-level auditing
3.  Entity / field-level auditing

All integration demonstrations are in:

``` text
src/test/java/io/changelens/demo/DemoProductIntegrationTest.java
```

## 1. Prerequisites

The demo requires the project's configured:

-   Java/JDK
-   Maven
-   PostgreSQL
-   Kafka
-   Redis, if required by the configured idempotency implementation

Check infrastructure with:

``` bash
docker ps
```

The test application should connect to the same PostgreSQL and Kafka
instances used by the demo.

## 2. SDK Overview

The application integrates with ChangeLens primarily through `@Audit`.

``` java
@Audit(
    action = "CREATE",
    resource = "CUSTOMER"
)
```

Conceptually:

``` mermaid
flowchart LR
    APP[Application] --> AOP[AuditAspect]
    AOP --> RESOLVER[Annotation Resolver]
    RESOLVER --> CONTEXT[AuditCaptureContext]
    CONTEXT --> FACTORY[AuditEventFactory]
    FACTORY --> EVENT[AuditEvent]
    EVENT --> PUBLISHER[AuditEventPublisher]
    PUBLISHER --> OUTBOX[Transactional Outbox]
```

The SDK keeps Kafka publication, asynchronous processing, idempotency,
retries and persistence out of the application's business methods.

## 3. Demo Application

The demo domain is intentionally small:

``` text
DemoCustomer
    |
    v
DemoCustomerService
    |
    +-- createCustomer()
    +-- updateCustomer()
    +-- deleteCustomer()
    +-- getCustomer()
```

This allows the test to demonstrate ChangeLens rather than application
complexity.

## 4. Test Class

The primary integration test class is:

``` text
DemoProductIntegrationTest
```

The three demonstration tests are:

``` text
shouldAuditCustomerCreation
shouldUseClassLevelAuditConfiguration
shouldCaptureEntityFieldChanges
```

## 5. Method-Level Auditing

### Application configuration

``` java
@Transactional
@Audit(
    action = "CREATE",
    resource = "CUSTOMER"
)
public DemoCustomer createCustomer(
        CreateCustomerRequest request) {
    ...
}
```

The method explicitly declares:

``` text
CREATE + CUSTOMER
```

### Test

``` text
shouldAuditCustomerCreation
```

### Run

``` bash
mvn -Dtest=DemoProductIntegrationTest#shouldAuditCustomerCreation test
```

### Expected flow

``` text
createCustomer()
    ↓
AuditAspect
    ↓
AuditEvent
    ↓
OutboxEvent = PENDING
    ↓
Outbox Scheduler
    ↓
Kafka
    ↓
AuditEventProcessor
    ↓
AuditProcessing = PROCESSED
    ↓
audit_event
```

A successful run should show the customer creation, an outbox event,
publication, Kafka processing and persisted audit event.

Expected audit information:

``` text
Action      = CREATE
Resource    = CUSTOMER
Resource ID = created customer ID
Status      = PROCESSED
```

## 6. Class-Level Auditing

### Application configuration

``` java
@Audit(
    action = "UPDATE",
    resource = "CUSTOMER"
)
public class DemoCustomerService {
    ...
}
```

`updateCustomer()` does not need to repeat the same audit declaration.

### Test

``` text
shouldUseClassLevelAuditConfiguration
```

### Run

``` bash
mvn -Dtest=DemoProductIntegrationTest#shouldUseClassLevelAuditConfiguration test
```

### Expected result

The audit event should contain:

``` text
Action      = UPDATE
Resource    = CUSTOMER
Resource ID = updated customer ID
Status      = PROCESSED
```

This demonstrates service-level audit configuration.

## 7. Entity / Field-Level Auditing

### Scenario

The demo changes:

``` text
BEFORE

Name   = Abhishek
Email  = abhishek@example.com
Status = ACTIVE
```

to:

``` text
AFTER

Name   = Abhishek Gupta
Email  = abhishek.gupta@example.com
Status = PREMIUM
```

The intended field-level result is:

``` text
name:
    Abhishek -> Abhishek Gupta

email:
    abhishek@example.com -> abhishek.gupta@example.com

status:
    ACTIVE -> PREMIUM
```

### Test

``` text
shouldCaptureEntityFieldChanges
```

### Run

``` bash
mvn -Dtest=DemoProductIntegrationTest#shouldCaptureEntityFieldChanges test
```

### Expected architecture

``` mermaid
flowchart TD
    UPDATE[Entity Update]
    BEFORE[Before State]
    AFTER[After State]
    COMPARE[Change Detection]
    CHANGES[Field Changes]
    EVENT[AuditEvent]
    OUTBOX[Outbox]
    KAFKA[Kafka]
    PROCESS[Audit Processing]
    STORE[Audit Store]

    UPDATE --> BEFORE
    UPDATE --> AFTER
    BEFORE --> COMPARE
    AFTER --> COMPARE
    COMPARE --> CHANGES
    CHANGES --> EVENT
    EVENT --> OUTBOX
    OUTBOX --> KAFKA
    KAFKA --> PROCESS
    PROCESS --> STORE
```

### MVP status

The current MVP contains the domain and persistence foundation for:

``` text
beforeState
afterState
fieldChanges
```

The generic entity state capture/change detection mechanism is the
remaining refinement area.

Therefore, if the entity test currently reports:

``` text
Entity audit should contain after-state
Expecting actual not to be null
```

the Outbox/Kafka/processing pipeline has already completed; the missing
behavior is entity state capture/change detection.

## 8. Run All Three Individually

For a live demonstration, run them separately:

``` bash
mvn -Dtest=DemoProductIntegrationTest#shouldAuditCustomerCreation test
```

``` bash
mvn -Dtest=DemoProductIntegrationTest#shouldUseClassLevelAuditConfiguration test
```

``` bash
mvn -Dtest=DemoProductIntegrationTest#shouldCaptureEntityFieldChanges test
```

Or run the complete class:

``` bash
mvn -Dtest=DemoProductIntegrationTest test
```

## 9. Database Verification

Inspect the public tables:

``` sql
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema = 'public'
ORDER BY table_name;
```

Inspect audit events:

``` sql
SELECT *
FROM audit_event
ORDER BY event_timestamp DESC;
```

Inspect processing:

``` sql
SELECT *
FROM audit_processing
ORDER BY processed_at DESC;
```

Inspect outbox:

``` sql
SELECT *
FROM outbox_event
ORDER BY created_at DESC;
```

Inspect field-level changes:

``` sql
SELECT *
FROM audit_change
ORDER BY id DESC;
```

Inspect demo customers:

``` sql
SELECT *
FROM demo_customer
ORDER BY id DESC;
```

## 10. Understanding a Successful Pipeline

For method-level and class-level auditing, the expected progression is:

``` text
1. Business operation executes
2. AuditAspect intercepts operation
3. AuditEvent is created
4. OutboxEvent is created
5. OutboxEvent = PENDING
6. Scheduler publishes event
7. OutboxEvent = PUBLISHED
8. Kafka delivers event
9. AuditProcessing record is created
10. Processing = PROCESSED
11. AuditEvent is persisted
```

## 11. Troubleshooting

### No outbox event

Check:

``` sql
SELECT *
FROM outbox_event
ORDER BY created_at DESC;
```

If no event exists, inspect:

``` text
AuditAspect
    ↓
AuditEventFactory
    ↓
AuditEventPublisher
    ↓
Outbox
```

### Outbox remains PENDING

Check:

-   `OutboxPublishingScheduler`
-   Kafka availability
-   Kafka producer configuration

Expected:

``` text
PENDING -> PUBLISHED
```

### No audit processing record

Check:

-   Kafka topic `audit-events`
-   Kafka consumer
-   `AuditEventProcessor`

Expected table:

``` text
audit_processing
```

### Processing failed

Inspect:

``` sql
SELECT *
FROM audit_processing
ORDER BY event_id DESC;
```

Then inspect application logs for the processing exception.

### Audit processing is PROCESSED but audit event is missing

Check:

``` text
AuditProcessingService
    ↓
AuditProcessingStatusService
    ↓
AuditEventMapper
    ↓
AuditEventRepository
```

### Entity test reports null state

If the output shows:

``` text
Action = UPDATE
Resource = CUSTOMER
Status = PROCESSED
```

but `afterState` is null, the asynchronous pipeline is working.
Investigate entity state capture/change detection:

``` text
AuditAspect
    ↓
AuditCaptureContext
    ↓
AuditEventFactory
    ↓
beforeState / afterState
```

## 12. Why These Are Integration Tests

The tests intentionally exercise the complete pipeline instead of
mocking the infrastructure.

``` text
Business Operation
       ↓
Audit Capture
       ↓
Outbox
       ↓
Scheduler
       ↓
Kafka
       ↓
Processing
       ↓
Persistence
       ↓
Assertions
```

This makes `DemoProductIntegrationTest` an end-to-end demonstration of
the ChangeLens MVP.

## 13. Recommended Demo Order

### 1. Method-level

``` bash
mvn -Dtest=DemoProductIntegrationTest#shouldAuditCustomerCreation test
```

Explain that an individual method explicitly declares its audit intent.

### 2. Class-level

``` bash
mvn -Dtest=DemoProductIntegrationTest#shouldUseClassLevelAuditConfiguration test
```

Explain that a service can define reusable audit configuration.

### 3. Entity / field-level

``` bash
mvn -Dtest=DemoProductIntegrationTest#shouldCaptureEntityFieldChanges test
```

Explain that the goal is to move from knowing that an entity changed to
knowing exactly which fields changed.

## 14. Key Takeaway

The application-side experience is intentionally small:

``` java
@Audit(
    action = "CREATE",
    resource = "CUSTOMER"
)
```

while ChangeLens owns:

``` text
Audit Capture
    ↓
Audit Event
    ↓
Transactional Outbox
    ↓
Kafka
    ↓
Idempotency
    ↓
Processing
    ↓
Retry / DLQ
    ↓
Audit Store
```

That separation is the core architectural idea behind the ChangeLens
MVP.
