package config

import (
	"fmt"
	"os"
	"strings"
	"time"
)

type Config struct {
	HTTPAddr           string
	DatabaseURL        string
	KafkaBrokers       []string
	EventsTopic        string
	AcksTopic          string
	DLQTopic           string
	KafkaGroupID       string
	OutboxPollInterval time.Duration
}

func Load() (Config, error) {
	pollInterval, err := time.ParseDuration(env("OUTBOX_POLL_INTERVAL", "1s"))
	if err != nil || pollInterval <= 0 {
		return Config{}, fmt.Errorf("OUTBOX_POLL_INTERVAL must be a positive duration")
	}

	brokers := splitCSV(env("KAFKA_BROKERS", "localhost:9092"))
	if len(brokers) == 0 {
		return Config{}, fmt.Errorf("KAFKA_BROKERS must include at least one broker")
	}

	return Config{
		HTTPAddr:           env("HTTP_ADDR", ":8080"),
		DatabaseURL:        env("DATABASE_URL", "postgres://orders:orders@localhost:5432/orders?sslmode=disable"),
		KafkaBrokers:       brokers,
		EventsTopic:        env("KAFKA_EVENTS_TOPIC", "order-events"),
		AcksTopic:          env("KAFKA_ACKS_TOPIC", "order-created"),
		DLQTopic:           env("KAFKA_DLQ_TOPIC", "order-events-dlq"),
		KafkaGroupID:       env("KAFKA_GROUP_ID", "order-service"),
		OutboxPollInterval: pollInterval,
	}, nil
}

func env(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok && strings.TrimSpace(value) != "" {
		return value
	}
	return fallback
}

func splitCSV(value string) []string {
	values := strings.Split(value, ",")
	result := make([]string, 0, len(values))
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}
