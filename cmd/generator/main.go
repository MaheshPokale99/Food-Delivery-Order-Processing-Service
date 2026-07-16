package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"

	"github.com/vaurd/food-delivery-order-service/internal/domain"
)

type generator struct {
	producer *kafka.Writer
	acks     *kafka.Reader
	topic    string
	rate     time.Duration
	random   *rand.Rand
	logger   *slog.Logger
	mu       sync.RWMutex
	orders   map[uuid.UUID]domain.Status
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("generator stopped", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	brokers := splitCSV(env("KAFKA_BROKERS", "localhost:9092"))
	if len(brokers) == 0 {
		return fmt.Errorf("KAFKA_BROKERS must include at least one broker")
	}
	rate, err := strconv.Atoi(env("EVENTS_PER_SECOND", "2"))
	if err != nil || rate < 1 || rate > 100 {
		return fmt.Errorf("EVENTS_PER_SECOND must be between 1 and 100")
	}
	eventsTopic := env("KAFKA_EVENTS_TOPIC", "order-events")
	acksTopic := env("KAFKA_ACKS_TOPIC", "order-created")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	g := &generator{
		producer: kafka.NewWriter(kafka.WriterConfig{
			Brokers:      brokers,
			Topic:        eventsTopic,
			Balancer:     &kafka.Hash{},
			RequiredAcks: int(kafka.RequireAll),
		}),
		acks: kafka.NewReader(kafka.ReaderConfig{
			Brokers:     brokers,
			GroupID:     env("KAFKA_GENERATOR_GROUP_ID", "order-generator"),
			Topic:       acksTopic,
			StartOffset: kafka.FirstOffset,
			MinBytes:    1,
			MaxBytes:    10e6,
			MaxWait:     500 * time.Millisecond,
		}),
		topic:  eventsTopic,
		rate:   time.Second / time.Duration(rate),
		random: rand.New(rand.NewSource(time.Now().UnixNano())),
		logger: logger,
		orders: make(map[uuid.UUID]domain.Status),
	}
	defer g.producer.Close()
	defer g.acks.Close()

	// Wait for service-created IDs before sending updates.
	go g.consumeAcknowledgements(ctx)
	ticker := time.NewTicker(g.rate)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			event := g.nextEvent()
			for {
				if err := g.publish(ctx, event); err == nil {
					break
				} else {
					g.logger.Warn("Kafka unavailable, retrying event", "error", err)
				}
				select {
				case <-ctx.Done():
					return nil
				case <-time.After(time.Second):
				}
			}
		}
	}
}

func (g *generator) consumeAcknowledgements(ctx context.Context) {
	for {
		message, err := g.acks.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			g.logger.Error("fetch order acknowledgement", "error", err)
			continue
		}
		var acknowledgement struct {
			EventID    uuid.UUID `json:"eventId"`
			Type       string    `json:"type"`
			OccurredAt time.Time `json:"occurredAt"`
			Data       struct {
				SourceEventID uuid.UUID `json:"sourceEventId"`
				OrderID       uuid.UUID `json:"orderId"`
			} `json:"data"`
		}
		if err := decodeStrict(message.Value, &acknowledgement); err != nil ||
			acknowledgement.Type != "order.created" || acknowledgement.EventID == uuid.Nil ||
			acknowledgement.OccurredAt.IsZero() || acknowledgement.Data.OrderID == uuid.Nil {
			g.logger.Error("invalid order acknowledgement", "error", err)
			if commitErr := g.acks.CommitMessages(ctx, message); commitErr != nil {
				g.logger.Error("commit invalid order acknowledgement", "error", commitErr)
			}
			continue
		}
		g.mu.Lock()
		g.orders[acknowledgement.Data.OrderID] = domain.StatusReceived
		g.mu.Unlock()
		if err := g.acks.CommitMessages(ctx, message); err != nil {
			g.logger.Error("commit order acknowledgement", "error", err)
		}
	}
}

func (g *generator) nextEvent() domain.Event {
	g.mu.RLock()
	known := make([]struct {
		id     uuid.UUID
		status domain.Status
	}, 0, len(g.orders))
	for id, status := range g.orders {
		known = append(known, struct {
			id     uuid.UUID
			status domain.Status
		}{id, status})
	}
	g.mu.RUnlock()

	if len(known) == 0 || g.random.Float64() < 0.4 {
		return g.createEvent()
	}
	order := known[g.random.Intn(len(known))]
	transitions := nextStatuses(order.status)
	if len(transitions) > 0 && g.random.Float64() < 0.55 {
		status := transitions[g.random.Intn(len(transitions))]
		g.mu.Lock()
		g.orders[order.id] = status
		g.mu.Unlock()
		return domain.Event{
			EventID:    uuid.New(),
			Type:       domain.EventOrderUpdateStatus,
			OccurredAt: time.Now().UTC(),
			Data:       mustJSON(domain.UpdateStatusData{OrderID: order.id, Status: status}),
		}
	}
	return domain.Event{
		EventID:    uuid.New(),
		Type:       domain.EventOrderUpdateItems,
		OccurredAt: time.Now().UTC(),
		Data:       mustJSON(domain.UpdateItemsData{OrderID: order.id, Items: g.randomItems()}),
	}
}

func (g *generator) createEvent() domain.Event {
	return domain.Event{
		EventID:    uuid.New(),
		Type:       domain.EventOrderCreate,
		OccurredAt: time.Now().UTC(),
		Data: mustJSON(domain.CreateOrderData{
			CustomerID:   fmt.Sprintf("customer-%d", g.random.Intn(500)+1),
			RestaurantID: pick(g.random, []string{"restaurant-amber", "restaurant-basil", "restaurant-citrus"}),
			Items:        g.randomItems(),
		}),
	}
}

func (g *generator) publish(ctx context.Context, event domain.Event) error {
	if err := event.Validate(); err != nil {
		return fmt.Errorf("validate generated event: %w", err)
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("encode generated event: %w", err)
	}
	key := event.EventID.String()
	if event.Type != domain.EventOrderCreate {
		// Key updates by order so Kafka preserves each order's event sequence.
		orderID, _, err := eventOrderID(event)
		if err != nil {
			return err
		}
		key = orderID.String()
	}
	if err := g.producer.WriteMessages(ctx, kafka.Message{Key: []byte(key), Value: payload}); err != nil {
		return fmt.Errorf("publish generated event: %w", err)
	}
	g.logger.Info("published event", "type", event.Type, "eventId", event.EventID)
	return nil
}

func (g *generator) randomItems() []domain.Item {
	catalog := []string{"burger", "pizza", "biryani", "salad", "pasta", "wrap"}
	selected := make(map[string]struct{})
	for len(selected) < g.random.Intn(4)+1 {
		selected[pick(g.random, catalog)] = struct{}{}
	}
	items := make([]domain.Item, 0, len(selected))
	for itemID := range selected {
		items = append(items, domain.Item{ItemID: itemID, Qty: g.random.Intn(4) + 1})
	}
	return items
}

func nextStatuses(status domain.Status) []domain.Status {
	switch status {
	case domain.StatusReceived:
		return []domain.Status{domain.StatusPreparing, domain.StatusCancelled}
	case domain.StatusPreparing:
		return []domain.Status{domain.StatusComplete, domain.StatusCancelled}
	default:
		return nil
	}
}

func eventOrderID(event domain.Event) (uuid.UUID, bool, error) {
	if event.Type == domain.EventOrderUpdateStatus {
		data, err := event.StatusData()
		return data.OrderID, true, err
	}
	data, err := event.ItemsData()
	return data.OrderID, true, err
}

func decodeStrict(payload []byte, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return fmt.Errorf("acknowledgement contains multiple JSON values")
	}
	return nil
}

func mustJSON(value any) json.RawMessage {
	payload, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return payload
}

func pick[T any](random *rand.Rand, values []T) T {
	return values[random.Intn(len(values))]
}

func env(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func splitCSV(value string) []string {
	parts := strings.Split(value, ",")
	brokers := make([]string, 0, len(parts))
	for _, part := range parts {
		if part = strings.TrimSpace(part); part != "" {
			brokers = append(brokers, part)
		}
	}
	return brokers
}
