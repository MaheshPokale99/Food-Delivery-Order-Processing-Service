# Design

## Architecture and flow

1. The Go generator builds a JSON event and validates it with the shared domain package before publishing to Kafka topic `order-events`.
2. The Go consumer strictly decodes and validates the event, then applies it in one PostgreSQL transaction.
3. A create transaction generates the service-owned order UUID, writes the current state, records the event ID, and writes an `order.created` acknowledgement to an outbox table.
4. The outbox publisher sends the acknowledgement to `order-created` and marks it published. The generator only emits updates after receiving this acknowledgement.
5. `GET /v1/orders` reads the current-state `orders` table directly.

The acknowledgement topic resolves the apparent tension in the assignment: the service, rather than the generator, creates `orderId`, while the generator can still send valid updates for real orders.

## Data model

`orders` is a current-state projection. It stores the customer and restaurant IDs, JSONB items, status, timestamps for the latest status and items events, and a `last_event_at` timestamp for ordering API results.

`processed_events` makes normal event handling idempotent. `pending_order_events` retains valid updates for an unknown order ID. `rejected_events` provides an audit trail for business-rule failures such as invalid status transitions. `outbox_events` avoids losing the creation acknowledgement between the database commit and Kafka publication.

The order state has separate event timestamps and IDs for status and items. A status update cannot overwrite a newer status just because an item update was received later, and vice versa. For equal timestamps, event UUID strings give a deterministic tie-breaker.

## Decisions for edge cases

### Out-of-order events

The normal generator sequence is create -> durable acknowledgement -> update, so an update is not emitted before the create is committed. An externally supplied update whose order does not exist is stored in `pending_order_events` instead of blocking a Kafka partition. If the referenced ID is later created, pending events are applied in timestamp order. Old events that arrive after a newer value are accepted as processed but do not replace the newer field.

### Concurrency

Kafka messages for an existing order use that order UUID as the key, preserving per-key partition order. The database transaction locks the target order row with `FOR UPDATE`, which protects correctness when multiple consumer replicas or retries contend for the same order. The API only sees committed rows.

### Duplicate and replayed events

The event UUID is inserted into `processed_events` in the same transaction as the projection update. A duplicate insert is a no-op, so offset commits can safely be retried and Kafka's at-least-once delivery does not create duplicate state changes. The acknowledgement outbox may publish more than once after a crash; the generator stores acknowledgements by order ID, making that harmless.

### Status transitions

The allowed lifecycle is:

```text
Received -> Preparing -> Complete
     |             |
     +-----------> Cancelled
```

An unchanged status is harmless. Terminal states cannot move backward. Invalid forward events are written to `rejected_events` and marked processed rather than retried forever.

### Throughput and scaling

The Compose setup creates six source-topic partitions. Run multiple API replicas with the same consumer group to consume them in parallel. PostgreSQL has indexes for status filtering and newest-first listing; the write path updates one row per event. The outbox claims messages with `FOR UPDATE SKIP LOCKED`, so multiple publishers can work without claiming the same row concurrently.

For substantially higher volume, use cursor pagination, connection-pool sizing, Kafka replication, monitoring, and partition keys that avoid hot orders. Production deployments should also configure Kafka TLS/SASL, PostgreSQL credentials through a secret manager, backups, and retention policies for dead-letter and audit tables.

### Latest-state guarantee

The consumer commits a Kafka offset only after the event transaction succeeds. The list API reads the same committed PostgreSQL projection that the consumer updates, so it never returns a partially applied order. Its guarantee is current state for all successfully consumed and committed events; it cannot include messages still waiting in Kafka or being processed.

## Failure behavior

Malformed events are sent to `order-events-dlq` and then committed so they cannot poison a consumer partition. Transient database or Kafka failures leave the source offset uncommitted, causing a safe retry. The create acknowledgement is durable in the database outbox before the source event is committed.
