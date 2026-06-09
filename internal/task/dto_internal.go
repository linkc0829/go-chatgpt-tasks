package task

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/linkc0829/go-chatgpt-tasks/internal/platform/postgres"
	"github.com/linkc0829/go-chatgpt-tasks/internal/platform/postgres/sqlc"
	"github.com/linkc0829/go-chatgpt-tasks/internal/shared"
)

func jobFromSqlc(r sqlc.GetJobByIDRow) *Job {
	return rehydrateJob(
		shared.JobID(postgres.PgToUUID(r.ID)),
		shared.TenantID(postgres.PgToUUID(r.TenantID)),
		shared.UserID(postgres.PgToUUID(r.UserID)),
		Kind(r.Kind),
		r.Description,
		time.Duration(r.IntervalSeconds)*time.Second,
		Kind(r.ScheduleType),
		postgres.PgToTime(r.ScheduledAtUtc),
		stringValue(r.RecurrenceRule),
		stringValue(r.LocalTime),
		r.TimezoneID,
		stringValue(r.OriginalUserText),
		r.SideEffecting,
		r.IdempotencyScope,
		jobIDPtrFromPg(r.ParentJobID),
		Status(stringValue(r.TriggerOnParentStatus)),
		JobType(r.JobType),
		postgres.PgToTime(r.CreatedAt),
		postgres.PgToTime(r.UpdatedAt),
	)
}

func jobFromChildRow(r sqlc.FindChildJobsRow) *Job {
	return rehydrateJob(
		shared.JobID(postgres.PgToUUID(r.ID)),
		shared.TenantID(postgres.PgToUUID(r.TenantID)),
		shared.UserID(postgres.PgToUUID(r.UserID)),
		Kind(r.Kind),
		r.Description,
		time.Duration(r.IntervalSeconds)*time.Second,
		Kind(r.ScheduleType),
		postgres.PgToTime(r.ScheduledAtUtc),
		stringValue(r.RecurrenceRule),
		stringValue(r.LocalTime),
		r.TimezoneID,
		stringValue(r.OriginalUserText),
		r.SideEffecting,
		r.IdempotencyScope,
		jobIDPtrFromPg(r.ParentJobID),
		Status(stringValue(r.TriggerOnParentStatus)),
		JobType(r.JobType),
		postgres.PgToTime(r.CreatedAt),
		postgres.PgToTime(r.UpdatedAt),
	)
}

func jobToInsertParams(j *Job) sqlc.InsertJobParams {
	return sqlc.InsertJobParams{
		ID:                    postgres.UUIDToPg(uuid.UUID(j.ID())),
		TenantID:              postgres.UUIDToPg(uuid.UUID(j.TenantID())),
		UserID:                postgres.UUIDToPg(uuid.UUID(j.UserID())),
		Kind:                  string(j.Kind()),
		Description:           j.Description(),
		IntervalSeconds:       int64(j.Interval() / time.Second),
		ScheduleType:          string(j.ScheduleType()),
		ScheduledAtUtc:        nullableTimeToPg(j.ScheduledAtUTC()),
		RecurrenceRule:        stringPtr(j.RecurrenceRule()),
		LocalTime:             stringPtr(j.LocalTime()),
		TimezoneID:            j.TimezoneID(),
		OriginalUserText:      stringPtr(j.OriginalUserText()),
		SideEffecting:         j.SideEffecting(),
		IdempotencyScope:      j.IdempotencyScope(),
		ParentJobID:           jobIDPtrToPg(j.ParentJobID()),
		TriggerOnParentStatus: stringPtr(string(j.TriggerOnParentStatus())),
		JobType:               string(j.JobType()),
		CreatedAt:             postgres.TimeToPg(j.CreatedAt()),
		UpdatedAt:             postgres.TimeToPg(j.UpdatedAt()),
	}
}

func jobRunFromGetByIDRow(r sqlc.GetJobRunByIDRow) *JobRun {
	return rehydrateJobRun(
		shared.JobRunID(postgres.PgToUUID(r.ID)),
		shared.TenantID(postgres.PgToUUID(r.TenantID)),
		shared.JobID(postgres.PgToUUID(r.JobID)),
		int(r.Sequence),
		Status(r.Status),
		postgres.PgToTime(r.ScheduledAt),
		r.TimeBucket,
		int(r.Attempts),
		r.IdempotencyKey,
		stringValue(r.ErrorCode),
		stringValue(r.ErrorMessage),
		postgres.PgToTime(r.StartedAt),
		postgres.PgToTime(r.CompletedAt),
		postgres.PgToTime(r.FailedAt),
		postgres.PgToTime(r.CreatedAt),
		postgres.PgToTime(r.UpdatedAt),
	)
}

func jobRunFromListRow(r sqlc.ListJobRunsRow) *JobRun {
	return rehydrateJobRun(
		shared.JobRunID(postgres.PgToUUID(r.ID)),
		shared.TenantID(postgres.PgToUUID(r.TenantID)),
		shared.JobID(postgres.PgToUUID(r.JobID)),
		int(r.Sequence),
		Status(r.Status),
		postgres.PgToTime(r.ScheduledAt),
		r.TimeBucket,
		int(r.Attempts),
		r.IdempotencyKey,
		stringValue(r.ErrorCode),
		stringValue(r.ErrorMessage),
		postgres.PgToTime(r.StartedAt),
		postgres.PgToTime(r.CompletedAt),
		postgres.PgToTime(r.FailedAt),
		postgres.PgToTime(r.CreatedAt),
		postgres.PgToTime(r.UpdatedAt),
	)
}

func jobRunFromDueRow(r sqlc.FindDueJobRunsRow) *JobRun {
	return rehydrateJobRun(
		shared.JobRunID(postgres.PgToUUID(r.ID)),
		shared.TenantID(postgres.PgToUUID(r.TenantID)),
		shared.JobID(postgres.PgToUUID(r.JobID)),
		int(r.Sequence),
		Status(r.Status),
		postgres.PgToTime(r.ScheduledAt),
		r.TimeBucket,
		int(r.Attempts),
		r.IdempotencyKey,
		stringValue(r.ErrorCode),
		stringValue(r.ErrorMessage),
		postgres.PgToTime(r.StartedAt),
		postgres.PgToTime(r.CompletedAt),
		postgres.PgToTime(r.FailedAt),
		postgres.PgToTime(r.CreatedAt),
		postgres.PgToTime(r.UpdatedAt),
	)
}

func jobRunFromJobRow(r sqlc.ListJobRunsByJobRow) *JobRun {
	return rehydrateJobRun(
		shared.JobRunID(postgres.PgToUUID(r.ID)),
		shared.TenantID(postgres.PgToUUID(r.TenantID)),
		shared.JobID(postgres.PgToUUID(r.JobID)),
		int(r.Sequence),
		Status(r.Status),
		postgres.PgToTime(r.ScheduledAt),
		r.TimeBucket,
		int(r.Attempts),
		r.IdempotencyKey,
		stringValue(r.ErrorCode),
		stringValue(r.ErrorMessage),
		postgres.PgToTime(r.StartedAt),
		postgres.PgToTime(r.CompletedAt),
		postgres.PgToTime(r.FailedAt),
		postgres.PgToTime(r.CreatedAt),
		postgres.PgToTime(r.UpdatedAt),
	)
}

func jobRunToInsertParams(r *JobRun) sqlc.InsertJobRunParams {
	return sqlc.InsertJobRunParams{
		ID:             postgres.UUIDToPg(uuid.UUID(r.ID())),
		TenantID:       postgres.UUIDToPg(uuid.UUID(r.TenantID())),
		JobID:          postgres.UUIDToPg(uuid.UUID(r.JobID())),
		Sequence:       int32(r.Sequence()), //nolint:gosec // domain validation keeps sequence positive and bounded by DB int use.
		Status:         string(r.Status()),
		ScheduledAt:    postgres.TimeToPg(r.ScheduledAt()),
		TimeBucket:     r.TimeBucket(),
		Attempts:       int32(r.Attempts()), //nolint:gosec // attempts is controlled by domain transitions and DB int use.
		IdempotencyKey: r.IdempotencyKey(),
		ErrorCode:      stringPtr(r.ErrorCode()),
		ErrorMessage:   stringPtr(r.ErrorMessage()),
		StartedAt:      nullableTimeToPg(r.StartedAt()),
		CompletedAt:    nullableTimeToPg(r.CompletedAt()),
		FailedAt:       nullableTimeToPg(r.FailedAt()),
		CreatedAt:      postgres.TimeToPg(r.CreatedAt()),
		UpdatedAt:      postgres.TimeToPg(r.UpdatedAt()),
	}
}

func jobRunToUpdateStatusParams(r *JobRun) sqlc.UpdateJobRunStatusParams {
	return sqlc.UpdateJobRunStatusParams{
		ID:           postgres.UUIDToPg(uuid.UUID(r.ID())),
		Status:       string(r.Status()),
		Attempts:     int32(r.Attempts()), //nolint:gosec // attempts is controlled by domain transitions and DB int use.
		ErrorCode:    stringPtr(r.ErrorCode()),
		ErrorMessage: stringPtr(r.ErrorMessage()),
		StartedAt:    nullableTimeToPg(r.StartedAt()),
		CompletedAt:  nullableTimeToPg(r.CompletedAt()),
		FailedAt:     nullableTimeToPg(r.FailedAt()),
		UpdatedAt:    postgres.TimeToPg(r.UpdatedAt()),
	}
}

func jobRunToInsertIfAbsentParams(r *JobRun) sqlc.InsertJobRunIfAbsentParams {
	return sqlc.InsertJobRunIfAbsentParams{
		ID:             postgres.UUIDToPg(uuid.UUID(r.ID())),
		TenantID:       postgres.UUIDToPg(uuid.UUID(r.TenantID())),
		JobID:          postgres.UUIDToPg(uuid.UUID(r.JobID())),
		Sequence:       int32(r.Sequence()), //nolint:gosec // domain validation keeps sequence positive and bounded by DB int use.
		ScheduledAt:    postgres.TimeToPg(r.ScheduledAt()),
		TimeBucket:     r.TimeBucket(),
		IdempotencyKey: r.IdempotencyKey(),
		CreatedAt:      postgres.TimeToPg(r.CreatedAt()),
		UpdatedAt:      postgres.TimeToPg(r.UpdatedAt()),
	}
}

func runEventToInsertParams(e *RunEvent) (sqlc.InsertRunEventParams, error) {
	payload, err := payloadToJSON(e.Payload())
	if err != nil {
		return sqlc.InsertRunEventParams{}, err
	}
	return sqlc.InsertRunEventParams{
		ID:           postgres.UUIDToPg(uuid.UUID(e.ID())),
		TenantID:     postgres.UUIDToPg(uuid.UUID(e.TenantID())),
		JobID:        postgres.UUIDToPg(uuid.UUID(e.JobID())),
		JobRunID:     postgres.UUIDToPg(uuid.UUID(e.JobRunID())),
		Status:       string(e.Status()),
		EventType:    string(e.EventType()),
		EventPayload: payload,
		CreatedAt:    postgres.TimeToPg(e.CreatedAt()),
	}, nil
}

func runEventFromSqlc(r sqlc.ListRunEventsByRunRow) (*RunEvent, error) {
	payload, err := payloadFromJSON(r.EventPayload)
	if err != nil {
		return nil, err
	}
	return &RunEvent{
		id:        shared.RunEventID(postgres.PgToUUID(r.ID)),
		tenantID:  shared.TenantID(postgres.PgToUUID(r.TenantID)),
		jobID:     shared.JobID(postgres.PgToUUID(r.JobID)),
		jobRunID:  shared.JobRunID(postgres.PgToUUID(r.JobRunID)),
		status:    Status(r.Status),
		eventType: EventType(r.EventType),
		payload:   payload,
		createdAt: postgres.PgToTime(r.CreatedAt),
	}, nil
}

func stringValue(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func stringPtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func nullableTimeToPg(t time.Time) pgtype.Timestamptz {
	if t.IsZero() {
		return pgtype.Timestamptz{}
	}
	return postgres.TimeToPg(t)
}

func jobIDPtrToPg(id *shared.JobID) pgtype.UUID {
	if id == nil {
		return pgtype.UUID{}
	}
	return postgres.UUIDToPg(uuid.UUID(*id))
}

func jobIDPtrFromPg(id pgtype.UUID) *shared.JobID {
	if !id.Valid {
		return nil
	}
	out := shared.JobID(postgres.PgToUUID(id))
	return &out
}

func payloadToJSON(payload map[string]any) ([]byte, error) {
	if payload == nil {
		return []byte("{}"), nil
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal event payload: %w", err)
	}
	return b, nil
}

func payloadFromJSON(b []byte) (map[string]any, error) {
	if len(b) == 0 {
		return nil, nil
	}
	var payload map[string]any
	if err := json.Unmarshal(b, &payload); err != nil {
		return nil, fmt.Errorf("decode event payload: %w", err)
	}
	return payload, nil
}

type NextRunSpec struct {
	TenantID       shared.TenantID
	JobID          shared.JobID
	Sequence       int
	ScheduledAt    time.Time
	TimezoneID     string
	RecurrenceRule string
	LocalTime      string
}

type JobRunMsg struct {
	JobRunID       string `json:"job_run_id"`
	TenantID       string `json:"tenant_id"`
	IdempotencyKey string `json:"idempotency_key"`
	Attempts       int    `json:"attempts"`
}

type IdempotencyRecord struct {
	Key          string
	Handler      string
	Status       string
	ResponseHash string
}

type HandlerInput struct {
	JobRunID       shared.JobRunID
	IdempotencyKey string
	TenantID       string
	JobType        string
	Payload        map[string]any
}

func nextRunSpecFromSqlc(r sqlc.ListTerminalRecurringRunsRow) NextRunSpec {
	return NextRunSpec{
		JobID:          shared.JobID(postgres.PgToUUID(r.JobID)),
		TenantID:       shared.TenantID(postgres.PgToUUID(r.TenantID)),
		Sequence:       int(r.Sequence),
		ScheduledAt:    postgres.PgToTime(r.ScheduledAt),
		TimezoneID:     r.TimezoneID,
		RecurrenceRule: stringValue(r.RecurrenceRule),
		LocalTime:      stringValue(r.LocalTime),
	}
}
