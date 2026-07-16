package domain

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/google/uuid"
)

type EventType string

const (
	EventOrderCreate       EventType = "order.create"
	EventOrderUpdateStatus EventType = "order.update.status"
	EventOrderUpdateItems  EventType = "order.update.items"
)

type Event struct {
	EventID    uuid.UUID       `json:"eventId"`
	Type       EventType       `json:"type"`
	OccurredAt time.Time       `json:"occurredAt"`
	Data       json.RawMessage `json:"data"`
}

type CreateOrderData struct {
	CustomerID   string `json:"customerId"`
	RestaurantID string `json:"restaurantId"`
	Items        []Item `json:"items"`
}

type UpdateStatusData struct {
	OrderID uuid.UUID `json:"orderId"`
	Status  Status    `json:"status"`
}

type UpdateItemsData struct {
	OrderID uuid.UUID `json:"orderId"`
	Items   []Item    `json:"items"`
}

func ParseEvent(payload []byte) (Event, error) {
	// Strict decoding keeps producer and consumer contracts aligned.
	var event Event
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&event); err != nil {
		return Event{}, fmt.Errorf("decode event: %w", err)
	}
	if err := ensureEOF(decoder); err != nil {
		return Event{}, err
	}
	if err := event.Validate(); err != nil {
		return Event{}, err
	}
	return event, nil
}

func (e Event) Validate() error {
	if e.EventID == uuid.Nil {
		return fmt.Errorf("eventId is required")
	}
	if e.OccurredAt.IsZero() {
		return fmt.Errorf("occurredAt is required")
	}
	if len(e.Data) == 0 {
		return fmt.Errorf("data is required")
	}

	switch e.Type {
	case EventOrderCreate:
		_, err := e.CreateData()
		return err
	case EventOrderUpdateStatus:
		_, err := e.StatusData()
		return err
	case EventOrderUpdateItems:
		_, err := e.ItemsData()
		return err
	default:
		return fmt.Errorf("unsupported event type %q", e.Type)
	}
}

func (e Event) CreateData() (CreateOrderData, error) {
	var data CreateOrderData
	if err := decodeStrict(e.Data, &data); err != nil {
		return CreateOrderData{}, fmt.Errorf("decode create data: %w", err)
	}
	if err := validateIdentifier("customerId", data.CustomerID); err != nil {
		return CreateOrderData{}, err
	}
	if err := validateIdentifier("restaurantId", data.RestaurantID); err != nil {
		return CreateOrderData{}, err
	}
	if err := validateItems(data.Items); err != nil {
		return CreateOrderData{}, err
	}
	return data, nil
}

func (e Event) StatusData() (UpdateStatusData, error) {
	var data UpdateStatusData
	if err := decodeStrict(e.Data, &data); err != nil {
		return UpdateStatusData{}, fmt.Errorf("decode status data: %w", err)
	}
	if data.OrderID == uuid.Nil {
		return UpdateStatusData{}, fmt.Errorf("orderId is required")
	}
	if !data.Status.Valid() {
		return UpdateStatusData{}, fmt.Errorf("status must be one of Received, Preparing, Complete, Cancelled")
	}
	return data, nil
}

func (e Event) ItemsData() (UpdateItemsData, error) {
	var data UpdateItemsData
	if err := decodeStrict(e.Data, &data); err != nil {
		return UpdateItemsData{}, fmt.Errorf("decode items data: %w", err)
	}
	if data.OrderID == uuid.Nil {
		return UpdateItemsData{}, fmt.Errorf("orderId is required")
	}
	if err := validateItems(data.Items); err != nil {
		return UpdateItemsData{}, err
	}
	return data, nil
}

func decodeStrict(payload []byte, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	return ensureEOF(decoder)
}

func ensureEOF(decoder *json.Decoder) error {
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return fmt.Errorf("only one JSON value is allowed")
		}
		return fmt.Errorf("decode JSON: %w", err)
	}
	return nil
}

func validateIdentifier(name, value string) error {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" || len(value) > 128 || trimmed != value {
		return fmt.Errorf("%s must be a non-empty trimmed string with at most 128 characters", name)
	}
	return nil
}

func validateItems(items []Item) error {
	if len(items) == 0 || len(items) > 50 {
		return fmt.Errorf("items must contain between 1 and 50 entries")
	}

	seen := make(map[string]struct{}, len(items))
	for _, item := range items {
		if err := validateIdentifier("itemId", item.ItemID); err != nil {
			return err
		}
		if item.Qty < 1 || item.Qty > 100 {
			return fmt.Errorf("qty must be between 1 and 100")
		}
		if _, exists := seen[item.ItemID]; exists {
			return fmt.Errorf("items must not contain duplicate itemId values")
		}
		seen[item.ItemID] = struct{}{}
	}
	return nil
}
