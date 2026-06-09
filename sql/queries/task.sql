-- name: InsertJob :exec
INSERT INTO jobs (id, tenant_id, user_id, kind, description, interval_seconds, schedule_type,
                  scheduled_at_utc, recurrence_rule, local_time, timezone_id, original_user_text,
                  side_effecting, idempotency_scope,
                  created_at, updated_at)
VALUES (sqlc.arg(id), sqlc.arg(tenant_id), sqlc.arg(user_id), sqlc.arg(kind),
        sqlc.arg(description), sqlc.arg(interval_seconds), sqlc.arg(schedule_type),
        sqlc.arg(scheduled_at_utc), sqlc.arg(recurrence_rule), sqlc.arg(local_time),
        sqlc.arg(timezone_id), sqlc.arg(original_user_text), sqlc.arg(side_effecting),
        sqlc.arg(idempotency_scope), sqlc.arg(created_at), sqlc.arg(updated_at));

-- name: InsertJobRun :exec
INSERT INTO job_runs (id, tenant_id, job_id, sequence, status, scheduled_at, time_bucket,
                      attempts, idempotency_key, error_code, error_message, started_at, completed_at, failed_at,
                      created_at, updated_at)
VALUES (sqlc.arg(id), sqlc.arg(tenant_id), sqlc.arg(job_id), sqlc.arg(sequence),
        sqlc.arg(status), sqlc.arg(scheduled_at), sqlc.arg(time_bucket),
        sqlc.arg(attempts), sqlc.arg(idempotency_key), sqlc.arg(error_code), sqlc.arg(error_message),
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
SELECT id, tenant_id, job_id, sequence, status, scheduled_at, time_bucket, attempts, idempotency_key,
       error_code, error_message, started_at, completed_at, failed_at, created_at, updated_at
FROM job_runs WHERE id = sqlc.arg(id);

-- name: ListJobRuns :many
SELECT id, tenant_id, job_id, sequence, status, scheduled_at, time_bucket, attempts, idempotency_key,
       error_code, error_message, started_at, completed_at, failed_at, created_at, updated_at
FROM job_runs
WHERE tenant_id = sqlc.arg(tenant_id)
ORDER BY created_at DESC
LIMIT sqlc.arg(page_limit) OFFSET sqlc.arg(page_offset);

-- name: CountJobRuns :one
SELECT COUNT(*) FROM job_runs WHERE tenant_id = sqlc.arg(tenant_id);

-- name: FindDueJobRuns :many
SELECT id, tenant_id, job_id, sequence, status, scheduled_at, time_bucket, attempts, idempotency_key,
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
SELECT id, tenant_id, job_id, sequence, status, scheduled_at, time_bucket, attempts, idempotency_key,
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
SELECT id, tenant_id, user_id, kind, description, interval_seconds, schedule_type,
       scheduled_at_utc, recurrence_rule, local_time, timezone_id, original_user_text,
       side_effecting, idempotency_scope,
       created_at, updated_at
FROM jobs WHERE id = sqlc.arg(id);

-- name: ListTerminalRecurringRuns :many
SELECT r.id, r.tenant_id, r.job_id, r.sequence, r.scheduled_at,
       j.timezone_id, j.recurrence_rule, j.local_time
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
                      attempts, idempotency_key, error_code, error_message, started_at, completed_at, failed_at,
                      created_at, updated_at)
VALUES (sqlc.arg(id), sqlc.arg(tenant_id), sqlc.arg(job_id), sqlc.arg(sequence), 'pending',
        sqlc.arg(scheduled_at), sqlc.arg(time_bucket), 0, sqlc.arg(idempotency_key),
        NULL, NULL, NULL, NULL, NULL,
        sqlc.arg(created_at), sqlc.arg(updated_at))
ON CONFLICT (job_id, sequence) DO NOTHING;

-- name: GetTenantQuota :one
SELECT max_jobs_per_hour, max_active_recurring_jobs, max_concurrent_runs,
       max_daily_llm_cost_cents
FROM tenant_quotas
WHERE tenant_id = sqlc.arg(tenant_id);

-- name: CountJobsCreatedSince :one
SELECT COUNT(*)
FROM jobs
WHERE tenant_id = sqlc.arg(tenant_id)
  AND created_at >= sqlc.arg(since);

-- name: CountActiveRecurringJobs :one
SELECT COUNT(*)
FROM jobs j
WHERE j.tenant_id = sqlc.arg(tenant_id)
  AND j.kind = 'recurring'
  AND EXISTS (
    SELECT 1
    FROM job_runs r
    WHERE r.job_id = j.id
      AND r.status NOT IN ('success', 'failed', 'cancelled')
  );

-- name: BeginIdempotency :execrows
INSERT INTO idempotency_records (
  idempotency_key, job_run_id, handler_name, status, created_at, updated_at
)
VALUES (
  sqlc.arg(idempotency_key), sqlc.arg(job_run_id), sqlc.arg(handler_name),
  'in_progress', sqlc.arg(created_at), sqlc.arg(updated_at)
)
ON CONFLICT (idempotency_key) DO NOTHING;

-- name: GetIdempotency :one
SELECT idempotency_key, handler_name, status, response_hash
FROM idempotency_records
WHERE idempotency_key = sqlc.arg(idempotency_key);

-- name: CompleteIdempotency :execrows
UPDATE idempotency_records
SET status = 'completed',
    response_hash = sqlc.arg(response_hash),
    updated_at = sqlc.arg(updated_at)
WHERE idempotency_key = sqlc.arg(idempotency_key);
