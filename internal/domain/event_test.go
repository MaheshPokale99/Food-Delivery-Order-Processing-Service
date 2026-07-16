package domain

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestEventValidation(t *testing.T) {
	event := Event{
		EventID:    uuid.New(),
		Type:       EventOrderCreate,
		OccurredAt: time.Now(),
		Data:       []byte(`{"customerId":"customer-1","restaurantId":"restaurant-1","items":[{"itemId":"pizza","qty":1}]}`),
	}

	if err := event.Validate(); err != nil {
		t.Fatalf("expected valid event, got %v", err)
	}
}

func TestEventRejectsDuplicateItems(t *testing.T) {
	event := Event{
		EventID:    uuid.New(),
		Type:       EventOrderCreate,
		OccurredAt: time.Now(),
		Data:       []byte(`{"customerId":"customer-1","restaurantId":"restaurant-1","items":[{"itemId":"pizza","qty":1},{"itemId":"pizza","qty":2}]}`),
	}

	if err := event.Validate(); err == nil {
		t.Fatal("expected duplicate item validation error")
	}
}

func TestStatusTransitions(t *testing.T) {
	if !CanTransition(StatusReceived, StatusPreparing) {
		t.Fatal("Received to Preparing should be valid")
	}
	if CanTransition(StatusComplete, StatusPreparing) {
		t.Fatal("Complete to Preparing should be invalid")
	}
}
