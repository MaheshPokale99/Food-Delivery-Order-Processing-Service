package httpapi

import (
	_ "embed"
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/vaurd/food-delivery-order-service/internal/domain"
	"github.com/vaurd/food-delivery-order-service/internal/repository"
)

//go:embed openapi.yaml
var openAPISpec []byte

type Server struct {
	repo   *repository.OrderRepository
	logger *slog.Logger
}

func New(repo *repository.OrderRepository, logger *slog.Logger) http.Handler {
	server := &Server{repo: repo, logger: logger}
	router := chi.NewRouter()
	router.Use(middleware.RequestID)
	router.Use(middleware.RealIP)
	router.Use(middleware.Logger)
	router.Use(middleware.Recoverer)
	router.Use(middleware.Timeout(10 * time.Second))
	router.Get("/healthz", server.health)
	router.Get("/readyz", server.ready)
	router.Get("/docs", server.docs)
	router.Get("/openapi.yaml", server.openapi)
	router.Get("/v1/orders", server.listOrders)
	return router
}

func (s *Server) health(writer http.ResponseWriter, request *http.Request) {
	writeJSON(writer, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) ready(writer http.ResponseWriter, request *http.Request) {
	if err := s.repo.Ping(request.Context()); err != nil {
		s.logger.Error("readiness database check failed", "error", err)
		writeError(writer, http.StatusServiceUnavailable, "service is not ready")
		return
	}
	writeJSON(writer, http.StatusOK, map[string]string{"status": "ready"})
}

func (s *Server) docs(writer http.ResponseWriter, request *http.Request) {
	writer.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = writer.Write([]byte(`<!doctype html>
<html lang="en"><head><meta charset="utf-8"><title>Order API Docs</title>
<link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5.17.14/swagger-ui.css"></head>
<body><div id="swagger-ui"></div>
<script src="https://unpkg.com/swagger-ui-dist@5.17.14/swagger-ui-bundle.js"></script>
<script>window.ui = SwaggerUIBundle({url: "/openapi.yaml", dom_id: "#swagger-ui"});</script>
</body></html>`))
}

func (s *Server) openapi(writer http.ResponseWriter, request *http.Request) {
	writer.Header().Set("Content-Type", "application/yaml; charset=utf-8")
	_, _ = writer.Write(openAPISpec)
}

func (s *Server) listOrders(writer http.ResponseWriter, request *http.Request) {
	// Parse limits at the edge so repository queries always receive safe values.
	options, err := parseListOptions(request)
	if err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}

	orders, total, err := s.repo.List(request.Context(), options)
	if err != nil {
		s.logger.Error("list orders", "error", err)
		writeError(writer, http.StatusInternalServerError, "could not list orders")
		return
	}
	if orders == nil {
		orders = make([]repository.Order, 0)
	}
	writeJSON(writer, http.StatusOK, struct {
		Data       []repository.Order `json:"data"`
		Pagination struct {
			Limit  int `json:"limit"`
			Offset int `json:"offset"`
			Total  int `json:"total"`
		} `json:"pagination"`
	}{
		Data: orders,
		Pagination: struct {
			Limit  int `json:"limit"`
			Offset int `json:"offset"`
			Total  int `json:"total"`
		}{Limit: options.Limit, Offset: options.Offset, Total: total},
	})
}

func parseListOptions(request *http.Request) (repository.ListOptions, error) {
	query := request.URL.Query()
	options := repository.ListOptions{Limit: 50, Offset: 0}

	if value := strings.TrimSpace(query.Get("status")); value != "" {
		status, err := domain.ParseStatus(value)
		if err != nil {
			return repository.ListOptions{}, err
		}
		options.Status = &status
	}
	if value := query.Get("limit"); value != "" {
		limit, err := strconv.Atoi(value)
		if err != nil || limit < 1 || limit > 100 {
			return repository.ListOptions{}, strconv.ErrSyntax
		}
		options.Limit = limit
	}
	if value := query.Get("offset"); value != "" {
		offset, err := strconv.Atoi(value)
		if err != nil || offset < 0 {
			return repository.ListOptions{}, strconv.ErrSyntax
		}
		options.Offset = offset
	}
	return options, nil
}

func writeError(writer http.ResponseWriter, status int, message string) {
	writeJSON(writer, status, map[string]string{"error": message})
}

func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.WriteHeader(status)
	if err := json.NewEncoder(writer).Encode(value); err != nil {
		return
	}
}
