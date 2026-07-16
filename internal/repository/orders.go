package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/vaurd/food-delivery-order-service/internal/domain"
)

type Order struct {
	ID            uuid.UUID     `json:"orderId"`
	CustomerID    string        `json:"customerId"`
	RestaurantID  string        `json:"restaurantId"`
	Items         []domain.Item `json:"items"`
	Status        domain.Status `json:"status"`
	LastUpdatedAt time.Time     `json:"lastUpdatedAt"`
	CreatedAt     time.Time     `json:"createdAt"`
}

type ListOptions struct {
	Status *domain.Status
	Limit  int
	Offset int
}

type OutboxMessage struct {
	ID      uuid.UUID
	Topic   string
	Key     string
	Payload []byte
}

type OrderRepository struct {
	pool      *pgxpool.Pool
	acksTopic string
}

func NewOrderRepository(pool *pgxpool.Pool, acksTopic string) *OrderRepository {
	return &OrderRepository{pool: pool, acksTopic: acksTopic}
}

func (r *OrderRepository) Ping(ctx context.Context) error {
	return r.pool.Ping(ctx)
}

func (r *OrderRepository) Apply(ctx context.Context, event domain.Event) error {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin apply transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	if orderID, isUpdate, err := eventOrderID(event); err != nil {
		return err
	} else if isUpdate {
		exists, err := orderExists(ctx, tx, orderID)
		if err != nil {
			return err
		}
		if !exists {
			// Keep the offset safe without blocking the partition on an unknown order.
			if err := savePending(ctx, tx, event, orderID); err != nil {
				return err
			}
			if err := tx.Commit(ctx); err != nil {
				return fmt.Errorf("commit pending event: %w", err)
			}
			return nil
		}
	}

	inserted, err := recordProcessed(ctx, tx, event)
	if err != nil {
		return err
	}
	if !inserted {
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit duplicate event: %w", err)
		}
		return nil
	}

	switch event.Type {
	case domain.EventOrderCreate:
		orderID, err := r.applyCreate(ctx, tx, event)
		if err != nil {
			return err
		}
		if err := r.drainPending(ctx, tx, orderID); err != nil {
			return err
		}
	case domain.EventOrderUpdateStatus:
		if err := r.applyStatus(ctx, tx, event); err != nil {
			return err
		}
	case domain.EventOrderUpdateItems:
		if err := r.applyItems(ctx, tx, event); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported event type %q", event.Type)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit event: %w", err)
	}
	return nil
}

func (r *OrderRepository) applyCreate(ctx context.Context, tx pgx.Tx, event domain.Event) (uuid.UUID, error) {
	data, err := event.CreateData()
	if err != nil {
		return uuid.Nil, err
	}
	items, err := json.Marshal(data.Items)
	if err != nil {
		return uuid.Nil, fmt.Errorf("encode order items: %w", err)
	}

	orderID := uuid.New()
	occurredAt := event.OccurredAt.UTC()
	_, err = tx.Exec(ctx, `
		INSERT INTO orders (
			id, customer_id, restaurant_id, items, status,
			status_event_at, status_event_id, items_event_at, items_event_id, last_event_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $6, $7, $6)`,
		orderID, data.CustomerID, data.RestaurantID, items, domain.StatusReceived,
		occurredAt, event.EventID,
	)
	if err != nil {
		return uuid.Nil, fmt.Errorf("insert order: %w", err)
	}

	acknowledgement := struct {
		EventID    uuid.UUID `json:"eventId"`
		Type       string    `json:"type"`
		OccurredAt time.Time `json:"occurredAt"`
		Data       struct {
			SourceEventID uuid.UUID `json:"sourceEventId"`
			OrderID       uuid.UUID `json:"orderId"`
		} `json:"data"`
	}{
		EventID:    uuid.New(),
		Type:       "order.created",
		OccurredAt: time.Now().UTC(),
	}
	acknowledgement.Data.SourceEventID = event.EventID
	acknowledgement.Data.OrderID = orderID
	payload, err := json.Marshal(acknowledgement)
	if err != nil {
		return uuid.Nil, fmt.Errorf("encode create acknowledgement: %w", err)
	}

	// The outbox keeps the acknowledgement durable across process crashes.
	_, err = tx.Exec(ctx, `
		INSERT INTO outbox_events (id, topic, message_key, payload)
		VALUES ($1, $2, $3, $4)`,
		uuid.New(), r.acksTopic, event.EventID.String(), payload,
	)
	if err != nil {
		return uuid.Nil, fmt.Errorf("enqueue create acknowledgement: %w", err)
	}
	return orderID, nil
}

func (r *OrderRepository) applyStatus(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	data, err := event.StatusData()
	if err != nil {
		return err
	}

	var currentStatus domain.Status
	var currentAt time.Time
	var currentEventID uuid.UUID
	err = tx.QueryRow(ctx, `
		SELECT status, status_event_at, status_event_id
		FROM orders WHERE id = $1 FOR UPDATE`, data.OrderID,
	).Scan(&currentStatus, &currentAt, &currentEventID)
	if err != nil {
		return fmt.Errorf("lock order for status update: %w", err)
	}
	if !isNewer(event.OccurredAt, event.EventID, currentAt, currentEventID) {
		return nil
	}
	if !domain.CanTransition(currentStatus, data.Status) {
		return rejectEvent(ctx, tx, event, fmt.Sprintf("invalid status transition from %s to %s", currentStatus, data.Status))
	}

	_, err = tx.Exec(ctx, `
		UPDATE orders
		SET status = $2,
			status_event_at = $3,
			status_event_id = $4,
			last_event_at = GREATEST(last_event_at, $3),
			updated_at = NOW()
		WHERE id = $1`,
		data.OrderID, data.Status, event.OccurredAt.UTC(), event.EventID,
	)
	if err != nil {
		return fmt.Errorf("update order status: %w", err)
	}
	return nil
}

func (r *OrderRepository) applyItems(ctx context.Context, tx pgx.Tx, event domain.Event) error {
	data, err := event.ItemsData()
	if err != nil {
		return err
	}

	var currentAt time.Time
	var currentEventID uuid.UUID
	err = tx.QueryRow(ctx, `
		SELECT items_event_at, items_event_id
		FROM orders WHERE id = $1 FOR UPDATE`, data.OrderID,
	).Scan(&currentAt, &currentEventID)
	if err != nil {
		return fmt.Errorf("lock order for item update: %w", err)
	}
	if !isNewer(event.OccurredAt, event.EventID, currentAt, currentEventID) {
		return nil
	}

	items, err := json.Marshal(data.Items)
	if err != nil {
		return fmt.Errorf("encode replacement items: %w", err)
	}
	_, err = tx.Exec(ctx, `
		UPDATE orders
		SET items = $2,
			items_event_at = $3,
			items_event_id = $4,
			last_event_at = GREATEST(last_event_at, $3),
			updated_at = NOW()
		WHERE id = $1`,
		data.OrderID, items, event.OccurredAt.UTC(), event.EventID,
	)
	if err != nil {
		return fmt.Errorf("update order items: %w", err)
	}
	return nil
}

func (r *OrderRepository) drainPending(ctx context.Context, tx pgx.Tx, orderID uuid.UUID) error {
	rows, err := tx.Query(ctx, `
		SELECT event_id, payload
		FROM pending_order_events
		WHERE order_id = $1
		ORDER BY occurred_at, event_id
		FOR UPDATE`, orderID)
	if err != nil {
		return fmt.Errorf("load pending order events: %w", err)
	}
	defer rows.Close()

	type pendingEvent struct {
		id      uuid.UUID
		payload []byte
	}
	pending := make([]pendingEvent, 0)
	for rows.Next() {
		var eventID uuid.UUID
		var payload []byte
		if err := rows.Scan(&eventID, &payload); err != nil {
			return fmt.Errorf("scan pending order event: %w", err)
		}
		pending = append(pending, pendingEvent{id: eventID, payload: payload})
	}
	if err := rows.Err(); err != nil {
		return err
	}
	rows.Close()

	for _, pendingEvent := range pending {
		eventID := pendingEvent.id
		payload := pendingEvent.payload
		event, err := domain.ParseEvent(payload)
		if err != nil {
			return fmt.Errorf("parse pending event %s: %w", eventID, err)
		}
		inserted, err := recordProcessed(ctx, tx, event)
		if err != nil {
			return err
		}
		if inserted {
			switch event.Type {
			case domain.EventOrderUpdateStatus:
				err = r.applyStatus(ctx, tx, event)
			case domain.EventOrderUpdateItems:
				err = r.applyItems(ctx, tx, event)
			}
			if err != nil {
				return err
			}
		}
		if _, err := tx.Exec(ctx, `DELETE FROM pending_order_events WHERE event_id = $1`, eventID); err != nil {
			return fmt.Errorf("delete applied pending event: %w", err)
		}
	}
	return nil
}

func (r *OrderRepository) List(ctx context.Context, options ListOptions) ([]Order, int, error) {
	status := any(nil)
	if options.Status != nil {
		status = string(*options.Status)
	}

	var total int
	if err := r.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM orders
		WHERE ($1::order_status IS NULL OR status = $1::order_status)`, status,
	).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count orders: %w", err)
	}

	rows, err := r.pool.Query(ctx, `
		SELECT id, customer_id, restaurant_id, items, status, last_event_at, created_at
		FROM orders
		WHERE ($1::order_status IS NULL OR status = $1::order_status)
		ORDER BY last_event_at DESC, id DESC
		LIMIT $2 OFFSET $3`, status, options.Limit, options.Offset,
	)
	if err != nil {
		return nil, 0, fmt.Errorf("query orders: %w", err)
	}
	defer rows.Close()

	orders := make([]Order, 0)
	for rows.Next() {
		var order Order
		var items []byte
		if err := rows.Scan(
			&order.ID, &order.CustomerID, &order.RestaurantID, &items,
			&order.Status, &order.LastUpdatedAt, &order.CreatedAt,
		); err != nil {
			return nil, 0, fmt.Errorf("scan order: %w", err)
		}
		if err := json.Unmarshal(items, &order.Items); err != nil {
			return nil, 0, fmt.Errorf("decode order items: %w", err)
		}
		orders = append(orders, order)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("iterate orders: %w", err)
	}
	return orders, total, nil
}

func (r *OrderRepository) ClaimOutbox(ctx context.Context, limit int) ([]OutboxMessage, error) {
	rows, err := r.pool.Query(ctx, `
		WITH candidates AS (
			SELECT id
			FROM outbox_events
			WHERE published_at IS NULL
				AND (locked_until IS NULL OR locked_until < NOW())
			ORDER BY created_at
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE outbox_events AS o
		SET locked_until = NOW() + INTERVAL '30 seconds', attempts = o.attempts + 1
		FROM candidates
		WHERE o.id = candidates.id
		RETURNING o.id, o.topic, o.message_key, o.payload`, limit)
	if err != nil {
		return nil, fmt.Errorf("claim outbox messages: %w", err)
	}
	defer rows.Close()

	messages := make([]OutboxMessage, 0)
	for rows.Next() {
		var message OutboxMessage
		if err := rows.Scan(&message.ID, &message.Topic, &message.Key, &message.Payload); err != nil {
			return nil, fmt.Errorf("scan outbox message: %w", err)
		}
		messages = append(messages, message)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate outbox messages: %w", err)
	}
	return messages, nil
}

func (r *OrderRepository) MarkOutboxPublished(ctx context.Context, id uuid.UUID) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE outbox_events
		SET published_at = NOW(), locked_until = NULL
		WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("mark outbox message published: %w", err)
	}
	return nil
}

func (r *OrderRepository) ReleaseOutbox(ctx context.Context, id uuid.UUID) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE outbox_events SET locked_until = NOW() WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("release outbox message: %w", err)
	}
	return nil
}

func eventOrderID(event domain.Event) (uuid.UUID, bool, error) {
	switch event.Type {
	case domain.EventOrderUpdateStatus:
		data, err := event.StatusData()
		return data.OrderID, true, err
	case domain.EventOrderUpdateItems:
		data, err := event.ItemsData()
		return data.OrderID, true, err
	default:
		return uuid.Nil, false, nil
	}
}

func orderExists(ctx context.Context, tx pgx.Tx, orderID uuid.UUID) (bool, error) {
	var exists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM orders WHERE id = $1)`, orderID).Scan(&exists); err != nil {
		return false, fmt.Errorf("check order existence: %w", err)
	}
	return exists, nil
}

func savePending(ctx context.Context, tx pgx.Tx, event domain.Event, orderID uuid.UUID) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("encode pending event: %w", err)
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO pending_order_events (event_id, order_id, event_type, occurred_at, payload)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (event_id) DO NOTHING`,
		event.EventID, orderID, event.Type, event.OccurredAt.UTC(), payload,
	)
	if err != nil {
		return fmt.Errorf("save pending event: %w", err)
	}
	return nil
}

func recordProcessed(ctx context.Context, tx pgx.Tx, event domain.Event) (bool, error) {
	// The unique event ID makes retries and replays harmless.
	var inserted bool
	err := tx.QueryRow(ctx, `
		INSERT INTO processed_events (event_id, event_type)
		VALUES ($1, $2)
		ON CONFLICT (event_id) DO NOTHING
		RETURNING TRUE`, event.EventID, event.Type).Scan(&inserted)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("record processed event: %w", err)
	}
	return inserted, nil
}

func rejectEvent(ctx context.Context, tx pgx.Tx, event domain.Event, reason string) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("encode rejected event: %w", err)
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO rejected_events (event_id, event_type, reason, payload)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (event_id) DO NOTHING`, event.EventID, event.Type, reason, payload)
	if err != nil {
		return fmt.Errorf("store rejected event: %w", err)
	}
	return nil
}

func isNewer(candidateAt time.Time, candidateID uuid.UUID, currentAt time.Time, currentID uuid.UUID) bool {
	candidateAt = candidateAt.UTC()
	currentAt = currentAt.UTC()
	return candidateAt.After(currentAt) ||
		(candidateAt.Equal(currentAt) && strings.Compare(candidateID.String(), currentID.String()) > 0)
}
