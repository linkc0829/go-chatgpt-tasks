package task

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/redis/go-redis/v9"
)

const (
	streamMain = "task:runs"
	streamDLQ  = "task:dlq"
	groupName  = "task-workers"
)

type RedisQueue struct {
	rdb *redis.Client
}

func NewRedisQueue(rdb *redis.Client) *RedisQueue {
	return &RedisQueue{rdb: rdb}
}

func (q *RedisQueue) Enqueue(ctx context.Context, m JobRunMsg) error {
	b, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("marshal msg: %w", err)
	}

	if err := q.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: streamMain,
		Values: map[string]any{"data": b},
	}).Err(); err != nil {
		return fmt.Errorf("xadd: %w", err)
	}
	return nil
}
