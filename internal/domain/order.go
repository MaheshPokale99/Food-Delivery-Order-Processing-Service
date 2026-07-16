package domain

import (
	"fmt"
)

type Status string

const (
	StatusReceived  Status = "Received"
	StatusPreparing Status = "Preparing"
	StatusComplete  Status = "Complete"
	StatusCancelled Status = "Cancelled"
)

type Item struct {
	ItemID string `json:"itemId"`
	Qty    int    `json:"qty"`
}

func ParseStatus(value string) (Status, error) {
	status := Status(value)
	if !status.Valid() {
		return "", fmt.Errorf("status must be one of Received, Preparing, Complete, Cancelled")
	}
	return status, nil
}

func (s Status) Valid() bool {
	switch s {
	case StatusReceived, StatusPreparing, StatusComplete, StatusCancelled:
		return true
	default:
		return false
	}
}

func CanTransition(from, to Status) bool {
	if from == to {
		return true
	}

	switch from {
	case StatusReceived:
		return to == StatusPreparing || to == StatusCancelled
	case StatusPreparing:
		return to == StatusComplete || to == StatusCancelled
	default:
		return false
	}
}
