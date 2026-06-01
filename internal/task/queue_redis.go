package task

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

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
	return q.xadd(ctx, streamMain, m)
}

func (q *RedisQueue) EnsureGroup(ctx context.Context) error {
	if err := q.rdb.XGroupCreateMkStream(ctx, streamMain, groupName, "0").Err(); err != nil {
		if strings.Contains(err.Error(), "BUSYGROUP") {
			return nil
		}
		return fmt.Errorf("xgroup create: %w", err)
	}
	return nil
}

func (q *RedisQueue) Read(
	ctx context.Context,
	consumer string,
	count int64,
	block time.Duration,
) ([]QueuedMessage, error) {
	streams, err := q.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    groupName,
		Consumer: consumer,
		Streams:  []string{streamMain, ">"},
		Count:    count,
		Block:    block,
	}).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, fmt.Errorf("xreadgroup: %w", err)
	}
	return queuedMessagesFromStreams(streams)
}

func (q *RedisQueue) Reclaim(
	ctx context.Context,
	consumer string,
	minIdle time.Duration,
	count int64,
) ([]QueuedMessage, error) {
	messages, _, err := q.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   streamMain,
		Group:    groupName,
		Consumer: consumer,
		MinIdle:  minIdle,
		Start:    "0",
		Count:    count,
	}).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, fmt.Errorf("xautoclaim: %w", err)
	}
	return queuedMessagesFromMessages(messages)
}

func (q *RedisQueue) Ack(ctx context.Context, streamID string) error {
	if err := q.rdb.XAck(ctx, streamMain, groupName, streamID).Err(); err != nil {
		return fmt.Errorf("xack: %w", err)
	}
	return nil
}

func (q *RedisQueue) DeadLetter(ctx context.Context, m JobRunMsg) error {
	return q.xadd(ctx, streamDLQ, m)
}

func (q *RedisQueue) xadd(ctx context.Context, stream string, m JobRunMsg) error {
	b, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("marshal msg: %w", err)
	}

	if err := q.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: map[string]any{"data": b},
	}).Err(); err != nil {
		return fmt.Errorf("xadd: %w", err)
	}
	return nil
}

func queuedMessagesFromStreams(streams []redis.XStream) ([]QueuedMessage, error) {
	var out []QueuedMessage
	for _, stream := range streams {
		msgs, err := queuedMessagesFromMessages(stream.Messages)
		if err != nil {
			return nil, err
		}
		out = append(out, msgs...)
	}
	return out, nil
}

func queuedMessagesFromMessages(messages []redis.XMessage) ([]QueuedMessage, error) {
	out := make([]QueuedMessage, 0, len(messages))
	for _, msg := range messages {
		runMsg, err := decodeJobRunMsg(msg.Values["data"])
		if err != nil {
			return nil, fmt.Errorf("decode redis stream message %s: %w", msg.ID, err)
		}
		out = append(out, QueuedMessage{StreamID: msg.ID, Msg: runMsg})
	}
	return out, nil
}

func decodeJobRunMsg(v any) (JobRunMsg, error) {
	var data []byte
	switch value := v.(type) {
	case string:
		data = []byte(value)
	case []byte:
		data = value
	default:
		return JobRunMsg{}, fmt.Errorf("unexpected data field type %T", v)
	}

	var msg JobRunMsg
	if err := json.Unmarshal(data, &msg); err != nil {
		return JobRunMsg{}, fmt.Errorf("unmarshal msg: %w", err)
	}
	return msg, nil
}
