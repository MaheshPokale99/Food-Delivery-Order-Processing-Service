package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"

	"github.com/vaurd/food-delivery-order-service/internal/domain"
	"github.com/vaurd/food-delivery-order-service/internal/repository"
)

type Consumer struct {
	reader *kafka.Reader
	dlq    *kafka.Writer
	repo   *repository.OrderRepository
	logger *slog.Logger
}

type OutboxPublisher struct {
	writer   *kafka.Writer
	repo     *repository.OrderRepository
	interval time.Duration
	logger   *slog.Logger
}

func NewConsumer(
	brokers []string,
	eventsTopic, dlqTopic, groupID string,
	repo *repository.OrderRepository,
	logger *slog.Logger,
) *Consumer {
	return &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:        brokers,
			GroupID:        groupID,
			Topic:          eventsTopic,
			StartOffset:    kafka.FirstOffset,
			MinBytes:       1,
			MaxBytes:       10e6,
			MaxWait:        500 * time.Millisecond,
			CommitInterval: 0,
		}),
		dlq:    newWriter(brokers, dlqTopic),
		repo:   repo,
		logger: logger,
	}
}

func NewOutboxPublisher(
	brokers []string,
	repo *repository.OrderRepository,
	interval time.Duration,
	logger *slog.Logger,
) *OutboxPublisher {
	return &OutboxPublisher{
		writer:   newWriter(brokers, ""),
		repo:     repo,
		interval: interval,
		logger:   logger,
	}
}

func (c *Consumer) Close() error {
	return errors.Join(c.reader.Close(), c.dlq.Close())
}

func (p *OutboxPublisher) Close() error {
	return p.writer.Close()
}

func (c *Consumer) Run(ctx context.Context) error {
	for {
		message, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			c.logger.Error("fetch Kafka message", "error", err)
			if err := waitForRetry(ctx); err != nil {
				return nil
			}
			continue
		}

		if err := c.process(ctx, message); err != nil {
			c.logger.Error("process Kafka message", "topic", message.Topic, "partition", message.Partition, "offset", message.Offset, "error", err)
			if err := waitForRetry(ctx); err != nil {
				return nil
			}
			continue
		}
		if err := c.reader.CommitMessages(ctx, message); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			c.logger.Error("commit Kafka message", "error", err)
		}
	}
}

func (c *Consumer) process(ctx context.Context, message kafka.Message) error {
	event, err := domain.ParseEvent(message.Value)
	if err != nil {
		// Permanent validation failures go to a separate topic, not an infinite retry loop.
		return c.sendToDLQ(ctx, message.Value, err.Error())
	}
	if err := c.repo.Apply(ctx, event); err != nil {
		return err
	}
	c.logger.Info("processed order event", "type", event.Type, "eventId", event.EventID)
	return nil
}

func (c *Consumer) sendToDLQ(ctx context.Context, payload []byte, reason string) error {
	deadLetter := struct {
		ID         uuid.UUID `json:"id"`
		Reason     string    `json:"reason"`
		ReceivedAt time.Time `json:"receivedAt"`
		Payload    string    `json:"payload"`
	}{
		ID:         uuid.New(),
		Reason:     reason,
		ReceivedAt: time.Now().UTC(),
		Payload:    string(payload),
	}
	encoded, err := json.Marshal(deadLetter)
	if err != nil {
		return fmt.Errorf("encode dead-letter message: %w", err)
	}
	if err := c.dlq.WriteMessages(ctx, kafka.Message{Key: []byte(deadLetter.ID.String()), Value: encoded}); err != nil {
		return fmt.Errorf("write dead-letter message: %w", err)
	}
	c.logger.Warn("sent invalid event to dead-letter topic", "reason", reason)
	return nil
}

func (p *OutboxPublisher) Run(ctx context.Context) error {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	for {
		if err := p.publishBatch(ctx); err != nil && ctx.Err() == nil {
			p.logger.Error("publish outbox batch", "error", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (p *OutboxPublisher) publishBatch(ctx context.Context) error {
	messages, err := p.repo.ClaimOutbox(ctx, 50)
	if err != nil {
		return err
	}
	for _, message := range messages {
		// The database row is marked published only after Kafka acknowledges the write.
		err := p.writer.WriteMessages(ctx, kafka.Message{Topic: message.Topic, Key: []byte(message.Key), Value: message.Payload})
		if err != nil {
			if releaseErr := p.repo.ReleaseOutbox(ctx, message.ID); releaseErr != nil {
				return errors.Join(fmt.Errorf("publish outbox message: %w", err), releaseErr)
			}
			return fmt.Errorf("publish outbox message: %w", err)
		}
		if err := p.repo.MarkOutboxPublished(ctx, message.ID); err != nil {
			return err
		}
		p.logger.Info("published outbox message", "topic", message.Topic, "messageId", message.ID)
	}
	return nil
}

func newWriter(brokers []string, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Topic:                  topic,
		Balancer:               &kafka.Hash{},
		RequiredAcks:           kafka.RequireAll,
		AllowAutoTopicCreation: false,
		BatchTimeout:           50 * time.Millisecond,
	}
}

func waitForRetry(ctx context.Context) error {
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
