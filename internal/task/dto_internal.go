package task

import (
	"time"

	"github.com/google/uuid"

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
		postgres.PgToTime(r.CreatedAt),
		postgres.PgToTime(r.UpdatedAt),
	)
}

func jobToInsertParams(j *Job) sqlc.InsertJobParams {
	return sqlc.InsertJobParams{
		ID:              postgres.UUIDToPg(uuid.UUID(j.ID())),
		TenantID:        postgres.UUIDToPg(uuid.UUID(j.TenantID())),
		UserID:          postgres.UUIDToPg(uuid.UUID(j.UserID())),
		Kind:            string(j.Kind()),
		Description:     j.Description(),
		IntervalSeconds: int64(j.Interval() / time.Second),
		CreatedAt:       postgres.TimeToPg(j.CreatedAt()),
		UpdatedAt:       postgres.TimeToPg(j.UpdatedAt()),
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
		postgres.PgToTime(r.CreatedAt),
		postgres.PgToTime(r.UpdatedAt),
	)
}

func jobRunToInsertParams(r *JobRun) sqlc.InsertJobRunParams {
	return sqlc.InsertJobRunParams{
		ID:          postgres.UUIDToPg(uuid.UUID(r.ID())),
		TenantID:    postgres.UUIDToPg(uuid.UUID(r.TenantID())),
		JobID:       postgres.UUIDToPg(uuid.UUID(r.JobID())),
		Sequence:    int32(r.Sequence()), //nolint:gosec // domain validation keeps sequence positive and bounded by DB int use.
		Status:      string(r.Status()),
		ScheduledAt: postgres.TimeToPg(r.ScheduledAt()),
		TimeBucket:  r.TimeBucket(),
		Attempts:    int32(r.Attempts()), //nolint:gosec // attempts is controlled by domain transitions and DB int use.
		CreatedAt:   postgres.TimeToPg(r.CreatedAt()),
		UpdatedAt:   postgres.TimeToPg(r.UpdatedAt()),
	}
}

func jobRunToUpdateStatusParams(r *JobRun) sqlc.UpdateJobRunStatusParams {
	return sqlc.UpdateJobRunStatusParams{
		ID:        postgres.UUIDToPg(uuid.UUID(r.ID())),
		Status:    string(r.Status()),
		Attempts:  int32(r.Attempts()), //nolint:gosec // attempts is controlled by domain transitions and DB int use.
		UpdatedAt: postgres.TimeToPg(r.UpdatedAt()),
	}
}

func jobRunToInsertIfAbsentParams(r *JobRun) sqlc.InsertJobRunIfAbsentParams {
	return sqlc.InsertJobRunIfAbsentParams{
		ID:          postgres.UUIDToPg(uuid.UUID(r.ID())),
		TenantID:    postgres.UUIDToPg(uuid.UUID(r.TenantID())),
		JobID:       postgres.UUIDToPg(uuid.UUID(r.JobID())),
		Sequence:    int32(r.Sequence()), //nolint:gosec // domain validation keeps sequence positive and bounded by DB int use.
		ScheduledAt: postgres.TimeToPg(r.ScheduledAt()),
		TimeBucket:  r.TimeBucket(),
		CreatedAt:   postgres.TimeToPg(r.CreatedAt()),
		UpdatedAt:   postgres.TimeToPg(r.UpdatedAt()),
	}
}

func runEventToInsertParams(e *RunEvent) sqlc.InsertRunEventParams {
	return sqlc.InsertRunEventParams{
		ID:        postgres.UUIDToPg(uuid.UUID(e.ID())),
		TenantID:  postgres.UUIDToPg(uuid.UUID(e.TenantID())),
		JobID:     postgres.UUIDToPg(uuid.UUID(e.JobID())),
		JobRunID:  postgres.UUIDToPg(uuid.UUID(e.JobRunID())),
		Status:    string(e.Status()),
		CreatedAt: postgres.TimeToPg(e.CreatedAt()),
	}
}

type NextRunSpec struct {
	TenantID    shared.TenantID
	JobID       shared.JobID
	Sequence    int
	ScheduledAt time.Time
	Interval    time.Duration
}

type JobRunMsg struct {
	JobRunID string `json:"job_run_id"`
	Attempts int    `json:"attempts"`
}

func nextRunSpecFromSqlc(r sqlc.ListTerminalRecurringRunsRow) NextRunSpec {
	return NextRunSpec{
		JobID:       shared.JobID(postgres.PgToUUID(r.JobID)),
		TenantID:    shared.TenantID(postgres.PgToUUID(r.TenantID)),
		Sequence:    int(r.Sequence),
		ScheduledAt: postgres.PgToTime(r.ScheduledAt),
		Interval:    time.Duration(r.IntervalSeconds) * time.Second,
	}
}
