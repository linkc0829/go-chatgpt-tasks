package task

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/linkc0829/go-chatgpt-tasks/internal/platform/postgres"
	"github.com/linkc0829/go-chatgpt-tasks/internal/platform/postgres/sqlc"
	"github.com/linkc0829/go-chatgpt-tasks/internal/shared"
)

type PostgresIdempotencyStore struct {
	q *sqlc.Queries
}

var _ IdempotencyStore = (*PostgresIdempotencyStore)(nil)

func NewIdempotencyStore(pool *pgxpool.Pool) *PostgresIdempotencyStore {
	return &PostgresIdempotencyStore{q: sqlc.New(pool)}
}

func (s *PostgresIdempotencyStore) Lookup(ctx context.Context, key string) (IdempotencyRecord, bool, error) {
	row, err := s.q.GetIdempotency(ctx, key)
	if errors.Is(err, pgx.ErrNoRows) {
		return IdempotencyRecord{}, false, nil
	}
	if err != nil {
		return IdempotencyRecord{}, false, fmt.Errorf("get idempotency record: %w", err)
	}
	return IdempotencyRecord{
		Key:          row.IdempotencyKey,
		Handler:      row.HandlerName,
		Status:       row.Status,
		ResponseHash: stringValue(row.ResponseHash),
	}, true, nil
}

func (s *PostgresIdempotencyStore) Begin(
	ctx context.Context,
	key string,
	handler string,
	runID shared.JobRunID,
) (bool, error) {
	now := postgres.TimeToPg(time.Now().UTC())
	rows, err := s.q.BeginIdempotency(ctx, sqlc.BeginIdempotencyParams{
		IdempotencyKey: key,
		JobRunID:       postgres.UUIDToPg(uuid.UUID(runID)),
		HandlerName:    handler,
		CreatedAt:      now,
		UpdatedAt:      now,
	})
	if err != nil {
		return false, fmt.Errorf("begin idempotency: %w", err)
	}
	return rows == 1, nil
}

func (s *PostgresIdempotencyStore) Complete(ctx context.Context, key, responseHash string) error {
	rows, err := s.q.CompleteIdempotency(ctx, sqlc.CompleteIdempotencyParams{
		IdempotencyKey: key,
		ResponseHash:   stringPtr(responseHash),
		UpdatedAt:      postgres.TimeToPg(time.Now().UTC()),
	})
	if err != nil {
		return fmt.Errorf("complete idempotency: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("complete idempotency: record not found")
	}
	return nil
}
