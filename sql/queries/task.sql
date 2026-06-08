-- name: InsertJob :exec
INSERT INTO jobs (id, tenant_id, user_id, kind, description, interval_seconds, created_at, updated_at)
VALUES (sqlc.arg(id), sqlc.arg(tenant_id), sqlc.arg(user_id), sqlc.arg(kind),
        sqlc.arg(description), sqlc.arg(interval_seconds), sqlc.arg(created_at),
        sqlc.arg(updated_at));

-- name: InsertJobRun :exec
INSERT INTO job_runs (id, tenant_id, job_id, sequence, status, scheduled_at, time_bucket,
                      attempts, error_code, error_message, started_at, completed_at, failed_at,
                      created_at, updated_at)
VALUES (sqlc.arg(id), sqlc.arg(tenant_id), sqlc.arg(job_id), sqlc.arg(sequence),
        sqlc.arg(status), sqlc.arg(scheduled_at), sqlc.arg(time_bucket),
        sqlc.arg(attempts), sqlc.arg(error_code), sqlc.arg(error_message),
        sqlc.arg(started_at), sqlc.arg(completed_at), sqlc.arg(failed_at),
        sqlc.arg(created_at), sqlc.arg(updated_at));

-- name: UpdateJobRunStatus :execrows
UPDATE job_runs
SET status = sqlc.arg(status),
    attempts = sqlc.arg(attempts),
    error_code = sqlc.arg(error_code),
    error_message = sqlc.arg(error_message),
    started_at = sqlc.arg(started_at),
    completed_at = sqlc.arg(completed_at),
    failed_at = sqlc.arg(failed_at),
    updated_at = sqlc.arg(updated_at)
WHERE id = sqlc.arg(id);

-- name: GetJobRunByID :one
SELECT id, tenant_id, job_id, sequence, status, scheduled_at, time_bucket, attempts,
       error_code, error_message, started_at, completed_at, failed_at, created_at, updated_at
FROM job_runs WHERE id = sqlc.arg(id);

-- name: ListJobRuns :many
SELECT id, tenant_id, job_id, sequence, status, scheduled_at, time_bucket, attempts,
       error_code, error_message, started_at, completed_at, failed_at, created_at, updated_at
FROM job_runs
WHERE tenant_id = sqlc.arg(tenant_id)
ORDER BY created_at DESC
LIMIT sqlc.arg(page_limit) OFFSET sqlc.arg(page_offset);

-- name: CountJobRuns :one
SELECT COUNT(*) FROM job_runs WHERE tenant_id = sqlc.arg(tenant_id);

-- name: FindDueJobRuns :many
SELECT id, tenant_id, job_id, sequence, status, scheduled_at, time_bucket, attempts,
       error_code, error_message, started_at, completed_at, failed_at, created_at, updated_at
FROM job_runs
WHERE time_bucket <= sqlc.arg(time_bucket)
  AND status = 'pending'
  AND scheduled_at <= sqlc.arg(due_before)
ORDER BY scheduled_at
LIMIT sqlc.arg(lim);

-- name: InsertRunEvent :exec
INSERT INTO run_events (id, tenant_id, job_id, job_run_id, status, event_type, event_payload, created_at)
VALUES (sqlc.arg(id), sqlc.arg(tenant_id), sqlc.arg(job_id), sqlc.arg(job_run_id),
        sqlc.arg(status), sqlc.arg(event_type), sqlc.arg(event_payload), sqlc.arg(created_at));

-- name: ListJobRunsByJob :many
SELECT id, tenant_id, job_id, sequence, status, scheduled_at, time_bucket, attempts,
       error_code, error_message, started_at, completed_at, failed_at, created_at, updated_at
FROM job_runs
WHERE tenant_id = sqlc.arg(tenant_id)
  AND job_id = sqlc.arg(job_id)
ORDER BY sequence
LIMIT sqlc.arg(page_limit) OFFSET sqlc.arg(page_offset);

-- name: CountJobRunsByJob :one
SELECT COUNT(*) FROM job_runs
WHERE tenant_id = sqlc.arg(tenant_id)
  AND job_id = sqlc.arg(job_id);

-- name: ListRunEventsByRun :many
SELECT id, tenant_id, job_id, job_run_id, status, event_type, event_payload, created_at
FROM run_events
WHERE tenant_id = sqlc.arg(tenant_id)
  AND job_run_id = sqlc.arg(job_run_id)
ORDER BY created_at;

-- name: GetJobByID :one
SELECT id, tenant_id, user_id, kind, description, interval_seconds, created_at, updated_at
FROM jobs WHERE id = sqlc.arg(id);

-- name: ListTerminalRecurringRuns :many
SELECT r.id, r.tenant_id, r.job_id, r.sequence, r.scheduled_at, j.interval_seconds
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
INSERT INTO job_runs (id, tenant_id, job_id, sequence, status, scheduled_at, time_bucket,
                      attempts, error_code, error_message, started_at, completed_at, failed_at,
                      created_at, updated_at)
VALUES (sqlc.arg(id), sqlc.arg(tenant_id), sqlc.arg(job_id), sqlc.arg(sequence), 'pending',
        sqlc.arg(scheduled_at), sqlc.arg(time_bucket), 0,
        NULL, NULL, NULL, NULL, NULL,
        sqlc.arg(created_at), sqlc.arg(updated_at))
ON CONFLICT (job_id, sequence) DO NOTHING;
