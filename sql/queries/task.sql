-- name: InsertJob :exec
INSERT INTO jobs (id, kind, description, interval_seconds, created_at, updated_at)
VALUES (sqlc.arg(id), sqlc.arg(kind), sqlc.arg(description),
        sqlc.arg(interval_seconds), sqlc.arg(created_at), sqlc.arg(updated_at));

-- name: InsertJobRun :exec
INSERT INTO job_runs (id, job_id, sequence, status, scheduled_at, time_bucket,
                      attempts, created_at, updated_at)
VALUES (sqlc.arg(id), sqlc.arg(job_id), sqlc.arg(sequence), sqlc.arg(status),
        sqlc.arg(scheduled_at), sqlc.arg(time_bucket), sqlc.arg(attempts),
        sqlc.arg(created_at), sqlc.arg(updated_at));

-- name: UpdateJobRunStatus :execrows
UPDATE job_runs
SET status = sqlc.arg(status), attempts = sqlc.arg(attempts), updated_at = sqlc.arg(updated_at)
WHERE id = sqlc.arg(id);

-- name: GetJobRunByID :one
SELECT id, job_id, sequence, status, scheduled_at, time_bucket, attempts, created_at, updated_at
FROM job_runs WHERE id = sqlc.arg(id);

-- name: ListJobRuns :many
SELECT id, job_id, sequence, status, scheduled_at, time_bucket, attempts, created_at, updated_at
FROM job_runs ORDER BY created_at DESC
LIMIT sqlc.arg(page_limit) OFFSET sqlc.arg(page_offset);

-- name: CountJobRuns :one
SELECT COUNT(*) FROM job_runs;

-- name: FindDueJobRuns :many
SELECT id, job_id, sequence, status, scheduled_at, time_bucket, attempts, created_at, updated_at
FROM job_runs
WHERE time_bucket <= sqlc.arg(time_bucket)
  AND status = 'pending'
  AND scheduled_at <= sqlc.arg(due_before)
ORDER BY scheduled_at
LIMIT sqlc.arg(lim);

-- name: InsertRunEvent :exec
INSERT INTO run_events (id, job_run_id, status, created_at)
VALUES (sqlc.arg(id), sqlc.arg(job_run_id), sqlc.arg(status), sqlc.arg(created_at));

-- name: GetJobByID :one
SELECT id, kind, description, interval_seconds, created_at, updated_at
FROM jobs WHERE id = sqlc.arg(id);

-- name: ListTerminalRecurringRuns :many
SELECT r.id, r.job_id, r.sequence, r.scheduled_at, j.interval_seconds
FROM run_events e
JOIN job_runs r ON r.id = e.job_run_id
JOIN jobs     j ON j.id = r.job_id
WHERE e.status IN ('success','failed')
  AND j.kind = 'recurring'
  AND e.created_at >= sqlc.arg(since)
  AND NOT EXISTS (
        SELECT 1 FROM job_runs n WHERE n.job_id = r.job_id AND n.sequence = r.sequence + 1)
ORDER BY e.created_at
LIMIT sqlc.arg(lim);

-- name: InsertJobRunIfAbsent :execrows
INSERT INTO job_runs (id, job_id, sequence, status, scheduled_at, time_bucket,
                      attempts, created_at, updated_at)
VALUES (sqlc.arg(id), sqlc.arg(job_id), sqlc.arg(sequence), 'pending',
        sqlc.arg(scheduled_at), sqlc.arg(time_bucket), 0,
        sqlc.arg(created_at), sqlc.arg(updated_at))
ON CONFLICT (job_id, sequence) DO NOTHING;
