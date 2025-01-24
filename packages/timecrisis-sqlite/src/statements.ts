export const SQLiteStatements = {
  insertJob: `
    INSERT INTO jobs (
      id,
      type,
      status,
      data,
      priority,
      progress,
      attempts,
      max_retries,
      backoff_strategy,
      fail_reason,
      fail_count,
      execution_duration,
      reference_id,
      expires_at,
      locked_at,
      started_at,
      run_at,
      finished_at,
      created_at,
      updated_at
    ) VALUES (
      @id,
      @type,
      @status,
      @data,
      @priority,
      @progress,
      @attempts,
      @max_retries,
      @backoff_strategy,
      @fail_reason,
      @fail_count,
      @execution_duration,
      @reference_id,
      @expires_at,
      @locked_at,
      @started_at,
      @run_at,
      @finished_at,
      @created_at,
      @updated_at
    )
  `,

  selectJobById: `
    SELECT * FROM jobs WHERE id = ?
  `,

  updateJob: `
    UPDATE jobs
    SET
      type = @type,
      status = @status,
      data = @data,
      priority = @priority,
      progress = @progress,
      attempts = @attempts,
      max_retries = @max_retries,
      backoff_strategy = @backoff_strategy,
      fail_reason = @fail_reason,
      fail_count = @fail_count,
      execution_duration = @execution_duration,
      reference_id = @reference_id,
      expires_at = @expires_at,
      locked_at = @locked_at,
      started_at = @started_at,
      run_at = @run_at,
      finished_at = @finished_at,
      updated_at = @updated_at
    WHERE id = @id
  `,

  deleteJob: `
    DELETE FROM jobs WHERE id = ?
  `,

  selectFilteredJobs: `
    SELECT *
    FROM jobs
    WHERE (@type IS NULL OR type = @type)
      AND (@referenceId IS NULL OR reference_id = @referenceId)
      AND (@lockedBefore IS NULL OR locked_at <= @lockedBefore)
      AND (run_at IS NULL OR (@runAtBefore IS NULL OR run_at <= @runAtBefore))
      AND (@status IS NULL OR status IN (SELECT value FROM json_each(@status)))
    ORDER BY priority DESC, created_at ASC
    LIMIT CASE WHEN @limit IS NULL THEN -1 ELSE @limit END
  `,

  insertJobRun: `
    INSERT INTO job_runs (
      id,
      job_id,
      status,
      started_at,
      progress,
      finished_at,
      attempt,
      error,
      error_stack
    ) VALUES (
      @id,
      @job_id,
      @status,
      @started_at,
      @progress,
      @finished_at,
      @attempt,
      @error,
      @error_stack
    )
  `,

  selectJobRunById: `
    SELECT * FROM job_runs WHERE id = ?
  `,

  updateJobRun: `
    UPDATE job_runs
    SET
      status = @status,
      started_at = @started_at,
      progress = @progress,
      finished_at = @finished_at,
      error = @error,
      error_stack = @error_stack,
      attempt = @attempt
    WHERE id = @id
  `,

  selectJobRunsByJobId: `
    SELECT * FROM job_runs WHERE job_id = ? ORDER BY started_at ASC
  `,

  deleteJobRunsByJobId: `
    DELETE FROM job_runs WHERE job_id = ?
  `,

  insertJobLog: `
    INSERT INTO job_logs (
      id,
      job_id,
      job_run_id,
      timestamp,
      level,
      message,
      metadata
    ) VALUES (
      @id,
      @job_id,
      @job_run_id,
      @timestamp,
      @level,
      @message,
      @metadata
    )
  `,

  selectJobLogsByJobId: `
    SELECT * FROM job_logs WHERE job_id = ? ORDER BY timestamp ASC
  `,

  selectJobLogsByJobAndRun: `
    SELECT * FROM job_logs WHERE job_id = ? AND job_run_id = ? ORDER BY timestamp ASC
  `,

  deleteJobLogsByJobId: `
    DELETE FROM job_logs WHERE job_id = ?
  `,

  insertScheduledJob: `
    INSERT INTO scheduled_jobs (
      id,
      name,
      type,
      schedule_type,
      schedule_value,
      data,
      enabled,
      last_scheduled_at,
      next_run_at,
      created_at,
      updated_at
    ) VALUES (
      @id,
      @name,
      @type,
      @schedule_type,
      @schedule_value,
      @data,
      @enabled,
      @last_scheduled_at,
      @next_run_at,
      @created_at,
      @updated_at
    )
  `,

  selectScheduledJobById: `
    SELECT * FROM scheduled_jobs WHERE id = ?
  `,

  updateScheduledJob: `
    UPDATE scheduled_jobs
    SET
      name = @name,
      type = @type,
      schedule_type = @schedule_type,
      schedule_value = @schedule_value,
      data = @data,
      enabled = @enabled,
      last_scheduled_at = @last_scheduled_at,
      next_run_at = @next_run_at,
      updated_at = @updated_at
    WHERE id = @id
  `,

  deleteScheduledJob: `
    DELETE FROM scheduled_jobs WHERE id = ?
  `,

  selectAllScheduledJobs: `
    SELECT * FROM scheduled_jobs
  `,

  selectFilteredScheduledJobs: `
    SELECT *
    FROM scheduled_jobs
    WHERE (enabled = @enabled OR @enabled IS NULL)
      AND (next_run_at <= @next_run_before OR @next_run_before IS NULL)
  `,

  insertDeadLetterJob: `
    INSERT INTO dead_letter_jobs (
      id,
      job_id,
      job_type,
      data,
      failed_at,
      reason
    ) VALUES (
      @id,
      @job_id,
      @job_type,
      @data,
      @failed_at,
      @reason
    )
  `,

  selectAllDeadLetterJobs: `
    SELECT * FROM dead_letter_jobs
  `,

  deleteDeadLetterBefore: `
    DELETE FROM dead_letter_jobs
    WHERE failed_at < @failed_at
  `,

  selectLock: `
    SELECT *
    FROM distributed_locks
    WHERE id = ?
  `,

  insertLock: `
    INSERT INTO distributed_locks (id, owner, acquired_at, expires_at, created_at)
    VALUES (@id, @owner, @acquired_at, @expires_at, @created_at)
    ON CONFLICT(id) DO UPDATE SET
      owner = @owner,
      acquired_at = @acquired_at,
      expires_at = @expires_at
    WHERE expires_at <= @acquired_at
  `,

  updateLock: `
    UPDATE distributed_locks
    SET expires_at = @expires_at
    WHERE id = @id AND owner = @owner
    AND (expires_at > @now OR owner = @owner)
  `,

  deleteLock: `
    DELETE FROM distributed_locks
    WHERE id = @id AND owner = @owner
  `,

  deleteExpiredLocks: `
    DELETE FROM distributed_locks
    WHERE expires_at <= @expires_at
  `,

  cleanupCompleted: `
    DELETE FROM jobs
    WHERE status = 'completed'
      AND updated_at < @updated_at
  `,

  cleanupFailed: `
    DELETE FROM jobs
    WHERE status = 'failed'
      AND updated_at < @updated_at
  `,

  jobCounts: `
    SELECT
      COUNT(*) as total,
      SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending,
      SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END) as running,
      SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
      SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed,
      SUM(CASE WHEN status = 'scheduled' THEN 1 ELSE 0 END) as scheduled,
      (SELECT COUNT(*) FROM dead_letter_jobs) as dead_letter
    FROM jobs
  `,

  avgDuration: `
    SELECT
      type,
      COALESCE(AVG(execution_duration), 0) AS avg_duration
    FROM jobs
    WHERE status = 'completed' AND execution_duration IS NOT NULL
    GROUP BY type
  `,

  failureRate: `
    SELECT
      type,
      CAST(
        COALESCE(
          CAST(SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS FLOAT) /
          NULLIF(COUNT(*), 0),
          0
        ) AS FLOAT
      ) as failure_rate
    FROM jobs
    WHERE status IN ('completed', 'failed')
    GROUP BY type
  `,
};
