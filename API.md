# API

Base URL: `http://localhost:8080`

Interactive Swagger UI: `GET /docs`  
Raw OpenAPI contract: `GET /openapi.yaml`

## GET /v1/orders

Returns the current state of orders from PostgreSQL. Results are sorted by `lastUpdatedAt` descending, then by `orderId` descending to make a stable page order.

### Query parameters

| Name | Type | Default | Rules |
| --- | --- | --- | --- |
| `status` | string | none | `Received`, `Preparing`, `Complete`, or `Cancelled` |
| `limit` | integer | `50` | 1 through 100 |
| `offset` | integer | `0` | zero or greater |

### Example

```http
GET /v1/orders?status=Preparing&limit=2&offset=0
```

```json
{
  "data": [
    {
      "orderId": "c1a4b3cb-fb53-4b85-a13f-5d4e17138566",
      "customerId": "customer-42",
      "restaurantId": "restaurant-basil",
      "items": [
        { "itemId": "pizza", "qty": 2 },
        { "itemId": "salad", "qty": 1 }
      ],
      "status": "Preparing",
      "lastUpdatedAt": "2026-07-16T10:15:30Z",
      "createdAt": "2026-07-16T10:14:00Z"
    }
  ],
  "pagination": {
    "limit": 2,
    "offset": 0,
    "total": 14
  }
}
```

Invalid parameters return `400`:

```json
{ "error": "status must be one of Received, Preparing, Complete, Cancelled" }
```

Database failures return `500` without exposing internal details.

## GET /healthz

Liveness endpoint. It confirms that the HTTP process is running.

```json
{ "status": "ok" }
```

## GET /readyz

Readiness endpoint. It verifies PostgreSQL connectivity and returns `503` when the database cannot be reached.

```json
{ "status": "ready" }
```

## Kafka event contract

All input events are JSON and are strictly decoded and validated by Go before processing. The generator uses the same shared Go domain validation before publishing.

### `order.create`

```json
{
  "eventId": "c587698c-4b58-4a58-8f17-dabed12c64fb",
  "type": "order.create",
  "occurredAt": "2026-07-16T10:14:00Z",
  "data": {
    "customerId": "customer-42",
    "restaurantId": "restaurant-basil",
    "items": [{ "itemId": "pizza", "qty": 2 }]
  }
}
```

The service generates the UUID `orderId` after it persists this event. It publishes an acknowledgement on `order-created`:

```json
{
  "eventId": "f775bda4-959d-466b-8542-278b224bcc6f",
  "type": "order.created",
  "occurredAt": "2026-07-16T10:14:01Z",
  "data": {
    "sourceEventId": "c587698c-4b58-4a58-8f17-dabed12c64fb",
    "orderId": "c1a4b3cb-fb53-4b85-a13f-5d4e17138566"
  }
}
```

### `order.update.status`

```json
{
  "eventId": "e3bfa27e-22cb-4af9-b7e3-e72b91b15e24",
  "type": "order.update.status",
  "occurredAt": "2026-07-16T10:15:00Z",
  "data": {
    "orderId": "c1a4b3cb-fb53-4b85-a13f-5d4e17138566",
    "status": "Preparing"
  }
}
```

### `order.update.items`

```json
{
  "eventId": "b2b982ac-9208-45e3-aa3c-d4258078c11d",
  "type": "order.update.items",
  "occurredAt": "2026-07-16T10:15:30Z",
  "data": {
    "orderId": "c1a4b3cb-fb53-4b85-a13f-5d4e17138566",
    "items": [{ "itemId": "pizza", "qty": 1 }, { "itemId": "salad", "qty": 1 }]
  }
}
```

`eventId`, `orderId`, and timestamps are required. Identifiers must be trimmed strings no longer than 128 characters; item lists contain 1 to 50 unique items; quantities are integers from 1 to 100.
