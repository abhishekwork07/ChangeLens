# ChangeLens --- MVP

ChangeLens is an application auditing platform designed to make auditing
a reusable platform capability rather than business logic implemented
independently in every application.

## MVP capabilities

-   Annotation-driven auditing
-   Method-level auditing
-   Class-level auditing
-   Entity / field-level auditing foundation
-   Standardized audit events
-   Transactional Outbox
-   Kafka-based asynchronous processing
-   Idempotent event processing
-   Processing status tracking
-   Retry and DLQ foundation
-   Persistent audit storage

## 1. Problem

Enterprise applications need to answer:

-   What changed?
-   Who changed it?
-   When did it change?
-   Which resource was affected?
-   What exactly changed at the field level?

ChangeLens separates **audit intent** from **audit infrastructure**. The
application declares what should be audited; ChangeLens manages the
audit event lifecycle.

## 2. High-Level Architecture

``` mermaid
flowchart LR
    APP[Business Application]
    SDK[ChangeLens SDK]
    OUTBOX[Transactional Outbox]
    SCHED[Outbox Publishing Scheduler]
    KAFKA[Kafka]
    PROCESSOR[Audit Event Processor]
    STORE[Audit Store]
    DLQ[Retry / DLQ]

    APP -->|@Audit / Entity Changes| SDK
    SDK -->|AuditEvent| OUTBOX
    OUTBOX --> SCHED
    SCHED -->|publish| KAFKA
    KAFKA --> PROCESSOR
    PROCESSOR -->|processed| STORE
    PROCESSOR -->|failure| DLQ
```

### Responsibilities

  ---------------------------------------------------------------------
  Component                          Responsibility
  ---------------------------------- ----------------------------------
  Business Application               Performs business operations and
                                     declares audit intent

  ChangeLens SDK                     Intercepts audited operations and
                                     creates standardized audit events

  Transactional Outbox               Reliably stores events with the
                                     business transaction

  Outbox Scheduler                   Publishes pending outbox events

  Kafka                              Provides asynchronous event
                                     transport

  Audit Event Processor              Validates, claims, processes and
                                     persists events

  Idempotency                        Prevents duplicate processing

  Audit Store                        Stores processed audit events and
                                     changes

  DLQ                                Handles events that cannot be
                                     successfully processed
  ---------------------------------------------------------------------

## 3. Audit Capture Architecture

``` mermaid
flowchart TD
    METHOD[Application Method]
    AOP[AuditAspect]
    RESOLVER[AuditAnnotationResolver]
    CONTEXT[AuditCaptureContext]
    FACTORY[AuditEventFactory]
    EVENT[AuditEvent]
    PUBLISHER[AuditEventPublisher]
    OUTBOX[Outbox]

    METHOD --> AOP
    AOP --> RESOLVER
    RESOLVER --> CONTEXT
    CONTEXT --> FACTORY
    FACTORY --> EVENT
    EVENT --> PUBLISHER
    PUBLISHER --> OUTBOX
```

The MVP supports method-level and class-level `@Audit` configuration.

### Method-level

``` java
@Audit(
    action = "CREATE",
    resource = "CUSTOMER"
)
public DemoCustomer createCustomer(...) {
    ...
}
```

### Class-level

``` java
@Audit(
    action = "UPDATE",
    resource = "CUSTOMER"
)
public class DemoCustomerService {
    ...
}
```

## 4. Audit Event Lifecycle

``` mermaid
sequenceDiagram
    participant App as Business Application
    participant SDK as ChangeLens SDK
    participant DB as PostgreSQL
    participant Scheduler as Outbox Scheduler
    participant Kafka
    participant Processor as Audit Processor
    participant AuditDB as Audit Store

    App->>SDK: Execute audited operation
    SDK->>SDK: Resolve @Audit
    SDK->>SDK: Create AuditEvent
    SDK->>DB: Persist OutboxEvent
    App-->>App: Business transaction completes
    Scheduler->>DB: Find PENDING events
    Scheduler->>Kafka: Publish AuditEvent
    Scheduler->>DB: Mark PUBLISHED
    Kafka->>Processor: Deliver AuditEvent
    Processor->>Processor: Deserialize + Validate
    Processor->>Processor: Idempotency claim
    Processor->>AuditDB: Mark processing / persist audit
    Processor->>Processor: Mark processed
```

## 5. Transactional Outbox

The business change and audit outbox event are committed together.

``` mermaid
sequenceDiagram
    participant Service as Business Service
    participant DB as PostgreSQL
    participant Scheduler as Outbox Scheduler
    participant Kafka

    Service->>DB: Save business entity
    Service->>DB: Save audit outbox event
    DB-->>Service: Commit
    Note over DB: Business change and outbox event<br/>are committed together
    Scheduler->>DB: Read PENDING events
    DB-->>Scheduler: Outbox event
    Scheduler->>Kafka: Publish event
    Kafka-->>Scheduler: Acknowledgement
    Scheduler->>DB: Mark PUBLISHED
```

This separates reliable event creation from asynchronous event
publication.

## 6. Kafka Processing

``` mermaid
flowchart TD
    KAFKA[Kafka: audit-events]
    CONSUMER[Kafka Consumer]
    DESERIALIZE[Deserialize]
    VALIDATE[Validate]
    CLAIM[Idempotency Claim]
    PROCESS[AuditProcessingService]
    FAILED[Processing Failure]
    RETRY[Retry]
    DLQ[DLQ]
    STORE[Audit Event Store]

    KAFKA --> CONSUMER
    CONSUMER --> DESERIALIZE
    DESERIALIZE --> VALIDATE
    VALIDATE --> CLAIM
    CLAIM --> PROCESS
    PROCESS --> STORE
    PROCESS --> FAILED
    FAILED --> RETRY
    RETRY -->|max attempts| DLQ
```

The processor validates events, performs an idempotency claim, processes
successful events, and routes unrecoverable failures toward DLQ
handling.

## 7. Persistence Model

Core MVP tables:

``` text
audit_event
audit_processing
audit_change
audit_dlq
outbox_event
demo_customer
flyway_schema_history
```

### `audit_event`

Stores event metadata, actor, resource, audit context, before/after
state and extensions.

### `audit_processing`

Tracks processing status, attempts, timestamps and errors.

### `audit_change`

Provides the persistence foundation for field-level changes.

### `audit_dlq`

Stores events that cannot be successfully processed after retries.

### `outbox_event`

Stores audit events before asynchronous Kafka publication.

## 8. Three Audit Capabilities

### Method-level auditing

``` mermaid
flowchart LR
    METHOD[createCustomer()] --> AUDIT[@Audit CREATE / CUSTOMER]
    AUDIT --> EVENT[AuditEvent]
    EVENT --> OUTBOX[Outbox]
    OUTBOX --> KAFKA[Kafka]
    KAFKA --> STORE[Audit Store]
```

A specific application method explicitly declares its audit intent.

### Class-level auditing

``` mermaid
flowchart LR
    CLASS[DemoCustomerService] --> CONFIG[@Audit UPDATE / CUSTOMER]
    CONFIG --> METHODS[Service Operations]
    METHODS --> EVENT[AuditEvent]
    EVENT --> PIPELINE[Audit Pipeline]
```

A service can define reusable audit configuration rather than repeating
the declaration on every operation.

### Entity / field-level auditing

``` mermaid
flowchart TD
    BEFORE[Before State]
    AFTER[After State]
    BEFORE --> COMPARE[Change Detection]
    AFTER --> COMPARE
    COMPARE --> CHANGES[Field Changes]
    CHANGES --> EVENT[AuditEvent]
```

The goal is to move beyond:

> Customer was updated.

toward:

``` text
name:
    Abhishek -> Abhishek Gupta

email:
    abhishek@example.com -> abhishek.gupta@example.com

status:
    ACTIVE -> PREMIUM
```

The MVP contains the domain and persistence foundation for
`beforeState`, `afterState`, and field-level changes. Generic entity
state capture/change detection is the next implementation refinement.

# 9. Detailed UML & Data Architecture

This section provides the principal UML, sequence, component, activity,
and ER diagrams for the MVP.

## 9.1 Component / UML Architecture

``` mermaid
classDiagram
    class BusinessApplication {
        +createCustomer()
        +updateCustomer()
        +deleteCustomer()
    }

    class AuditAspect {
        +audit(ProceedingJoinPoint)
    }

    class AuditAnnotationResolver {
        +resolve(Method, Class) Audit
    }

    class AuditCaptureContext {
        +Audit audit
        +AuditSource source
        +Method method
        +Object target
        +Object[] arguments
        +Object result
        +List~FieldChange~ fieldChanges
    }

    class AuditEventFactory {
        <<interface>>
        +create(AuditCaptureContext) AuditEvent
    }

    class AuditEventPublisher {
        <<interface>>
        +publish(AuditEvent)
    }

    class OutboxEvent {
        +UUID eventId
        +String payload
        +Status status
        +int attempts
    }

    class OutboxPublishingScheduler {
        +publishPendingEvents()
    }

    class Kafka {
        <<external>>
        +audit-events
    }

    class AuditEventProcessor {
        +process(String payload)
    }

    class IdempotencyService {
        +tryClaim(UUID eventId)
    }

    class AuditProcessingService {
        +process(AuditEvent)
        +markFailed(UUID eventId, String error)
    }

    class AuditProcessingStatusService {
        +markProcessed(AuditEvent)
        +markFailed(UUID eventId, String error)
    }

    class AuditEventRepository {
        +save(AuditEventEntity)
    }

    class AuditEventEntity {
        +UUID eventId
        +String action
        +String resourceType
        +String resourceId
        +Object beforeState
        +Object afterState
    }

    class AuditChange {
        +UUID eventId
        +String fieldName
        +Object oldValue
        +Object newValue
    }

    BusinessApplication --> AuditAspect : intercepted by
    AuditAspect --> AuditAnnotationResolver : resolves @Audit
    AuditAspect --> AuditCaptureContext : creates
    AuditCaptureContext --> AuditEventFactory : input
    AuditEventFactory --> AuditEventPublisher : creates/publishes
    AuditEventPublisher --> OutboxEvent : persists
    OutboxPublishingScheduler --> OutboxEvent : reads pending
    OutboxPublishingScheduler --> Kafka : publishes
    Kafka --> AuditEventProcessor : delivers
    AuditEventProcessor --> IdempotencyService : claims event
    AuditEventProcessor --> AuditProcessingService : processes
    AuditProcessingService --> AuditProcessingStatusService : updates status
    AuditProcessingStatusService --> AuditEventRepository : persists
    AuditEventRepository --> AuditEventEntity : stores
    AuditEventEntity --> AuditChange : field-level changes
```

## 9.2 End-to-End Sequence Diagram

This is the primary sequence for the MVP.

``` mermaid
sequenceDiagram
    autonumber

    participant App as Business Application
    participant Aspect as AuditAspect
    participant Resolver as AnnotationResolver
    participant Factory as AuditEventFactory
    participant Publisher as AuditEventPublisher
    participant DB as PostgreSQL
    participant Scheduler as OutboxPublishingScheduler
    participant Kafka as Kafka
    participant Consumer as Kafka Consumer
    participant Processor as AuditEventProcessor
    participant Idempotency as IdempotencyService
    participant Processing as AuditProcessingService
    participant AuditStore as Audit Store

    App->>Aspect: Invoke audited business method
    Aspect->>Resolver: Resolve @Audit
    Resolver-->>Aspect: Audit configuration
    Aspect->>App: proceed()
    App-->>Aspect: Business result

    Aspect->>Factory: create(AuditCaptureContext)
    Factory-->>Aspect: AuditEvent
    Aspect->>Publisher: publish(AuditEvent)
    Publisher->>DB: Insert OutboxEvent
    DB-->>Publisher: Outbox event persisted

    Note over App,DB: Business operation and outbox event<br/>are committed transactionally

    Scheduler->>DB: Find PENDING outbox events
    DB-->>Scheduler: Pending event
    Scheduler->>Kafka: Publish audit event
    Kafka-->>Scheduler: Publish acknowledgement
    Scheduler->>DB: Mark outbox event PUBLISHED

    Kafka->>Consumer: audit-events record
    Consumer->>Processor: process(payload)
    Processor->>Processor: Deserialize
    Processor->>Processor: Validate
    Processor->>Idempotency: tryClaim(eventId)
    Idempotency-->>Processor: STARTED

    Processor->>Processing: process(event)
    Processing->>AuditStore: markProcessed(event)
    AuditStore->>AuditStore: Persist audit_event
    AuditStore->>AuditStore: Persist audit_change
    AuditStore-->>Processing: Success
    Processing-->>Processor: Processed
```

## 9.3 Failure / Retry / DLQ Sequence

``` mermaid
sequenceDiagram
    autonumber

    participant Kafka as Kafka
    participant Processor as AuditEventProcessor
    participant Idempotency as IdempotencyService
    participant Processing as AuditProcessingService
    participant DB as PostgreSQL
    participant Retry as Kafka Retry Handler
    participant DLQ as Audit DLQ

    Kafka->>Processor: AuditEvent
    Processor->>Processor: Deserialize + Validate
    Processor->>Idempotency: tryClaim(eventId)
    Idempotency-->>Processor: STARTED

    Processor->>Processing: process(event)
    Processing->>DB: Process event

    DB-->>Processing: Processing failure
    Processing->>DB: markFailed(eventId, error)
    Processing-->>Processor: Exception

    Processor-->>Retry: Record failure
    Retry->>Kafka: Retry delivery

    Kafka->>Processor: Retry event
    Processor->>Idempotency: tryClaim(eventId)
    Idempotency-->>Processor: RETRY_STARTED

    Processor->>Processing: process(event)
    Processing-->>Processor: Failure

    Note over Retry,DLQ: After maximum attempts
    Retry->>DLQ: Move event to DLQ
```

## 9.4 Method-Level Audit Sequence

``` mermaid
sequenceDiagram
    participant Test as Integration Test
    participant Service as DemoCustomerService
    participant Aspect as AuditAspect
    participant Factory as AuditEventFactory
    participant Outbox as Outbox
    participant Scheduler as Scheduler
    participant Kafka
    participant Processor as AuditEventProcessor
    participant Store as Audit Store

    Test->>Service: createCustomer(request)
    Service->>Aspect: Intercept @Audit(CREATE, CUSTOMER)
    Aspect->>Service: proceed()
    Service-->>Aspect: DemoCustomer
    Aspect->>Factory: Create AuditEvent
    Factory-->>Aspect: CREATE / CUSTOMER
    Aspect->>Outbox: Save event
    Outbox-->>Aspect: PENDING

    Scheduler->>Outbox: Read PENDING
    Scheduler->>Kafka: Publish
    Kafka->>Processor: Deliver
    Processor->>Store: Persist audit event
    Store-->>Processor: Success
    Test->>Store: Assert audit event exists
```

## 9.5 Class-Level Audit Sequence

``` mermaid
sequenceDiagram
    participant Test as Integration Test
    participant Service as DemoCustomerService
    participant Aspect as AuditAspect
    participant Resolver as AnnotationResolver
    participant Pipeline as Audit Pipeline

    Test->>Service: updateCustomer()
    Service->>Aspect: Intercept method
    Aspect->>Resolver: Resolve method/class audit
    Resolver-->>Aspect: Class-level UPDATE / CUSTOMER
    Aspect->>Service: proceed()
    Service-->>Aspect: Updated Customer
    Aspect->>Pipeline: Create and publish AuditEvent
    Pipeline-->>Test: Audit persisted
```

## 9.6 Entity / Field-Level Change Sequence

``` mermaid
sequenceDiagram
    participant App as Business Application
    participant Aspect as AuditAspect
    participant Factory as AuditEventFactory
    participant Entity as DemoCustomer
    participant Detector as Change Detection
    participant Store as Audit Store

    App->>Aspect: updateCustomer()
    Aspect->>Entity: Capture operation/state
    Entity-->>Aspect: Updated entity/result
    Aspect->>Factory: AuditCaptureContext
    Factory->>Detector: Compare before/after state
    Detector-->>Factory: FieldChange list
    Factory-->>Aspect: AuditEvent
    Aspect->>Store: Persist audit event
    Store->>Store: Persist field changes
```

> **MVP note:** The architecture and domain model support `beforeState`,
> `afterState`, and `fieldChanges`. Generic automatic entity state
> capture/change detection is the remaining refinement area.

## 9.7 Audit Processing State Diagram

``` mermaid
stateDiagram-v2
    [*] --> STARTED
    STARTED --> PROCESSED: processing succeeds
    STARTED --> FAILED: processing fails

    FAILED --> RETRY_STARTED: retry available
    RETRY_STARTED --> PROCESSED: processing succeeds
    RETRY_STARTED --> FAILED: processing fails

    FAILED --> MAX_ATTEMPTS_REACHED: retry limit reached
    MAX_ATTEMPTS_REACHED --> DLQ

    STARTED --> ALREADY_PROCESSED: duplicate event
    RETRY_STARTED --> ALREADY_PROCESSED: duplicate event

    PROCESSED --> [*]
    ALREADY_PROCESSED --> [*]
    DLQ --> [*]
```

## 9.8 Audit Capture Activity Diagram

``` mermaid
flowchart TD
    START([Business Method Invoked])
    MATCH{Audit configuration found?}
    RESOLVE[Resolve method/class @Audit]
    PROCEED[Execute business method]
    CONTEXT[Create AuditCaptureContext]
    FACTORY[Create AuditEvent]
    PUBLISH[Publish AuditEvent]
    OUTBOX[Persist OutboxEvent]
    END([Business Operation Complete])
    SKIP[Continue without audit]

    START --> MATCH
    MATCH -->|No| SKIP
    MATCH -->|Yes| RESOLVE
    RESOLVE --> PROCEED
    PROCEED --> CONTEXT
    CONTEXT --> FACTORY
    FACTORY --> PUBLISH
    PUBLISH --> OUTBOX
    OUTBOX --> END
    SKIP --> END
```

# 10. Entity Relationship Diagram

The following ER diagram represents the main MVP persistence model and
its relationships.

``` mermaid
erDiagram

    DEMO_CUSTOMER {
        bigint id PK
        varchar name
        varchar email
        varchar status
    }

    OUTBOX_EVENT {
        uuid event_id PK
        text payload
        varchar status
        int attempts
        timestamp created_at
        timestamp published_at
        text last_error
    }

    AUDIT_PROCESSING {
        uuid event_id PK
        varchar status
        int attempts
        timestamp started_at
        timestamp processed_at
        timestamp failed_at
        text error
    }

    AUDIT_EVENT {
        uuid event_id PK
        int event_version
        varchar tenant_id
        varchar event_type
        timestamp event_timestamp
        varchar action
        varchar actor_type
        varchar actor_id
        varchar actor_name
        varchar resource_type
        varchar resource_id
        varchar resource_name
        varchar application_name
        varchar application_version
        varchar service_name
        varchar environment
        varchar request_id
        varchar correlation_id
        varchar trace_id
        varchar source_ip
        varchar user_agent
        jsonb before_state
        jsonb after_state
        jsonb extensions
        timestamp created_at
    }

    AUDIT_CHANGE {
        bigint id PK
        uuid event_id FK
        varchar field_name
        jsonb old_value
        jsonb new_value
    }

    AUDIT_DLQ {
        bigint id PK
        uuid event_id
        text payload
        text error
        int attempts
        timestamp created_at
    }

    FLYWAY_SCHEMA_HISTORY {
        varchar installed_rank PK
        varchar version
        varchar description
        varchar type
        varchar script
        timestamp installed_on
    }

    OUTBOX_EVENT ||--o| AUDIT_PROCESSING : "published event processed"
    AUDIT_EVENT ||--o{ AUDIT_CHANGE : "contains"
    AUDIT_PROCESSING ||--o| AUDIT_EVENT : "tracks"
    AUDIT_EVENT }o--o| DEMO_CUSTOMER : "references resource"
    AUDIT_EVENT ||--o| AUDIT_DLQ : "may enter"
```

### ER model interpretation

``` text
outbox_event
      |
      | asynchronous publication
      v
audit_processing
      |
      | successful processing
      v
audit_event
      |
      +------< audit_change
      |
      +------> referenced business resource
      |
      +------> audit_dlq (failure path)
```

`resource_id` is intentionally modeled as audit metadata rather than a
hard database foreign key to `demo_customer`, because ChangeLens is
intended to audit arbitrary application resources, not only the demo
customer entity.

## 10.1 Audit Event Data Structure

``` mermaid
classDiagram
    class AuditEvent {
        +UUID eventId
        +int eventVersion
        +String tenantId
        +AuditEventType eventType
        +Instant timestamp
        +String action
        +Actor actor
        +Resource resource
        +ChangeSet changeSet
        +Object beforeState
        +Object afterState
        +AuditContext context
        +Map extensions
    }

    class Actor {
        +ActorType type
        +String id
        +String name
    }

    class Resource {
        +String type
        +String id
        +String name
    }

    class ChangeSet {
        +String summary
        +List~FieldChange~ changes
    }

    class FieldChange {
        +String fieldName
        +Object oldValue
        +Object newValue
    }

    class AuditContext {
        +String applicationName
        +String applicationVersion
        +String serviceName
        +String environment
        +String requestId
        +String correlationId
        +String traceId
        +String sourceIp
        +String userAgent
    }

    AuditEvent --> Actor
    AuditEvent --> Resource
    AuditEvent --> ChangeSet
    AuditEvent --> AuditContext
    ChangeSet --> FieldChange
```

## 10.2 Persistence Mapping

The domain `AuditEvent` is mapped into `AuditEventEntity` before
persistence.

``` mermaid
flowchart LR
    DOMAIN[AuditEvent]
    MAPPER[AuditEventMapper]
    ENTITY[AuditEventEntity]
    REPO[AuditEventRepository]
    DB[(PostgreSQL)]

    DOMAIN --> MAPPER
    MAPPER --> ENTITY
    ENTITY --> REPO
    REPO --> DB
```

The entity stores actor, resource and context fields as relational
columns while `beforeState`, `afterState` and `extensions` use
PostgreSQL JSONB.

## 11. What the MVP Proves

The MVP establishes the foundation for a reusable auditing platform:

-   Annotation-driven audit configuration
-   Spring AOP-based interception
-   Standardized `AuditEvent`
-   Method-level auditing
-   Class-level auditing
-   Transactional Outbox
-   Asynchronous Kafka publication
-   Idempotency
-   Processing status tracking
-   Retry/DLQ foundation
-   Persistent audit store
-   Entity state / field-change model foundation

## 12. Future Direction

-   Complete generic entity state capture
-   Robust field-level change detection
-   Audit query APIs
-   Change Log dashboard
-   Search and filtering
-   Tenant-aware audit access
-   Operational monitoring
-   Additional integrations

## 13. Core Architectural Principle

> **The application declares what should be audited; ChangeLens owns the
> audit event lifecycle.**

This keeps audit infrastructure out of business logic and allows the
same platform to be reused across enterprise applications and resources.
