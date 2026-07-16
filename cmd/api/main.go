package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/vaurd/food-delivery-order-service/internal/config"
	"github.com/vaurd/food-delivery-order-service/internal/httpapi"
	"github.com/vaurd/food-delivery-order-service/internal/messaging"
	"github.com/vaurd/food-delivery-order-service/internal/repository"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("service stopped", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	config, err := config.Load()
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, config.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := waitForDatabase(ctx, pool, logger); err != nil {
		return err
	}

	repo := repository.NewOrderRepository(pool, config.AcksTopic)
	consumer := messaging.NewConsumer(config.KafkaBrokers, config.EventsTopic, config.DLQTopic, config.KafkaGroupID, repo, logger)
	defer consumer.Close()
	publisher := messaging.NewOutboxPublisher(config.KafkaBrokers, repo, config.OutboxPollInterval, logger)
	defer publisher.Close()

	server := &http.Server{
		Addr:              config.HTTPAddr,
		Handler:           httpapi.New(repo, logger),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	errorsChannel := make(chan error, 3)
	go func() { errorsChannel <- consumer.Run(ctx) }()
	go func() { errorsChannel <- publisher.Run(ctx) }()
	go func() {
		logger.Info("HTTP server listening", "address", config.HTTPAddr)
		err := server.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			errorsChannel <- nil
			return
		}
		errorsChannel <- err
	}()

	select {
	case err := <-errorsChannel:
		if err != nil {
			stop()
			shutdown(server, logger)
			return err
		}
	case <-ctx.Done():
		shutdown(server, logger)
	}

	return nil
}

func waitForDatabase(ctx context.Context, pool *pgxpool.Pool, logger *slog.Logger) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		if err := pool.Ping(ctx); err == nil {
			return nil
		} else {
			logger.Warn("waiting for PostgreSQL", "error", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func shutdown(server *http.Server, logger *slog.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		logger.Error("graceful HTTP shutdown failed", "error", err)
	}
}
