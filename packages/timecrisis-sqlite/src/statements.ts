export const SQLiteStatements = {
  insertJob: `
    INSERT INTO jobs (
      id,
      type,
      status,
      data,
      priority,
      max_retries,
      backoff_strategy,
      fail_reason,
      fail_count,
      reference_id,
      scheduled_job_id,
      expires_at,
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
      @max_retries,
      @backoff_strategy,
      @fail_reason,
      @fail_count,
      @reference_id,
      @scheduled_job_id,
      @expires_at,
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
      max_retries = @max_retries,
      backoff_strategy = @backoff_strategy,
      fail_reason = @fail_reason,
      fail_count = @fail_count,
      reference_id = @reference_id,
      scheduled_job_id = @scheduled_job_id,
      expires_at = @expires_at,
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
      AND (run_at IS NULL OR (@runAtBefore IS NULL OR run_at <= @runAtBefore))
      AND (@status IS NULL OR status IN (SELECT value FROM json_each(@status)))
      AND (@expiresAtBefore IS NULL OR (expires_at IS NOT NULL AND expires_at <= @expiresAtBefore))
    ORDER BY priority ASC, created_at ASC
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
      execution_duration,
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
      @execution_duration,
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
      execution_duration = @execution_duration,
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
      time_zone,
      data,
      enabled,
      last_scheduled_at,
      next_run_at,
      reference_id,
      created_at,
      updated_at
    ) VALUES (
      @id,
      @name,
      @type,
      @schedule_type,
      @schedule_value,
      @time_zone,
      @data,
      @enabled,
      @last_scheduled_at,
      @next_run_at,
      @reference_id,
      @created_at,
      @updated_at
    )
    ON CONFLICT(name, type) DO UPDATE SET
      schedule_type = excluded.schedule_type,
      schedule_value = excluded.schedule_value,
      time_zone = excluded.time_zone,
      data = excluded.data,
      enabled = excluded.enabled,
      last_scheduled_at = excluded.last_scheduled_at,
      next_run_at = excluded.next_run_at,
      reference_id = excluded.reference_id,
      updated_at = excluded.updated_at
    RETURNING id
  `,

  selectScheduledJobById: `
    SELECT * FROM scheduled_jobs WHERE id = ?
  `,

  selectScheduledJobByNameAndType: `
    SELECT * FROM scheduled_jobs WHERE name = @name AND type = @type
  `,

  updateScheduledJob: `
    UPDATE scheduled_jobs
    SET
      name = @name,
      type = @type,
      schedule_type = @schedule_type,
      schedule_value = @schedule_value,
      time_zone = @time_zone,
      data = @data,
      enabled = @enabled,
      last_scheduled_at = @last_scheduled_at,
      next_run_at = @next_run_at,
      reference_id = @reference_id,
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
      AND (@reference_id IS NULL OR reference_id = @reference_id)
  `,

  insertDeadLetterJob: `
    INSERT INTO dead_letter_jobs (
      id,
      job_id,
      job_type,
      data,
      failed_at,
      failed_reason
    ) VALUES (
      @id,
      @job_id,
      @job_type,
      @data,
      @failed_at,
      @failed_reason
    )
  `,

  selectAllDeadLetterJobs: `
    SELECT * FROM dead_letter_jobs
  `,

  deleteDeadLetterBefore: `
    DELETE FROM dead_letter_jobs
    WHERE failed_at < @failed_at
  `,

  listLocks: `
    SELECT id, worker, expires_at
    FROM distributed_locks
    WHERE (@worker IS NULL OR worker = @worker)
      AND (@expiredBefore IS NULL OR expires_at <= @expiredBefore)
  `,

  selectLock: `
    SELECT *
    FROM distributed_locks
    WHERE id = ?
  `,

  insertLock: `
    INSERT INTO distributed_locks (id, worker, acquired_at, expires_at, created_at)
    VALUES (@id, @worker, @now, @expires, @now)
    ON CONFLICT(id) DO UPDATE
      SET
        worker      = excluded.worker,
        acquired_at = excluded.acquired_at,
        expires_at  = excluded.expires_at
      /* only overwrite if it was expired */
      WHERE distributed_locks.expires_at <= excluded.acquired_at;
  `,

  updateLock: `
    UPDATE distributed_locks
    SET expires_at = @newExpiry
    WHERE id = @lockId
      AND worker = @worker
      AND expires_at > @now
  `,

  deleteLock: `
    DELETE FROM distributed_locks
    WHERE id = @id AND worker = @worker
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
      j.type,
      COALESCE(AVG(r.execution_duration), 0) AS avg_duration
    FROM jobs j
    LEFT JOIN job_runs r ON j.id = r.job_id
    WHERE r.status = 'completed' AND r.execution_duration IS NOT NULL
    GROUP BY j.type
  `,

  failureRate: `
    SELECT
      j.type,
      CAST(
        COALESCE(
          CAST(SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS FLOAT) /
          NULLIF(COUNT(*), 0),
          0
        ) AS FLOAT
      ) as failure_rate
    FROM jobs j
    WHERE status IN ('completed', 'failed')
    GROUP BY type
  `,

  // Worker statements
  insertWorker: `
    INSERT INTO workers (name, first_seen, last_heartbeat)
    VALUES (@name, @first_seen, @last_heartbeat)
    ON CONFLICT(name) DO UPDATE SET
      last_heartbeat = excluded.last_heartbeat
  `,

  updateWorkerHeartbeat: `
    UPDATE workers
    SET last_heartbeat = @last_heartbeat
    WHERE name = @name
  `,

  selectWorkerByName: `
    SELECT * FROM workers
    WHERE name = ?
  `,

  selectAllWorkers: `
    SELECT * FROM workers
  `,

  selectInactiveWorkers: `
    SELECT * FROM workers
    WHERE last_heartbeat < ?
  `,

  deleteWorker: `
    DELETE FROM workers
    WHERE name = ?
  `,

  // Job type slot statements
  upsertJobTypeSlot: `
    INSERT INTO job_type_slots (job_type, worker, slot_count)
    VALUES (@job_type, @worker, 1)
    ON CONFLICT (job_type, worker)
    DO UPDATE SET slot_count = slot_count + 1
  `,

  getTotalJobTypeSlots: `
    SELECT COALESCE(SUM(slot_count), 0) as total
    FROM job_type_slots
    WHERE job_type = ?
  `,

  getTotalRunningJobsByType: `
    SELECT COALESCE(SUM(slot_count), 0) as total
    FROM job_type_slots
    WHERE job_type = ?
  `,

  getTotalRunningJobs: `
    SELECT COALESCE(SUM(slot_count), 0) as total
    FROM job_type_slots
  `,

  decrementJobTypeSlot: `
    UPDATE job_type_slots
    SET slot_count = slot_count - 1
    WHERE job_type = @job_type AND worker = @worker
    AND slot_count > 0
  `,

  deleteEmptyJobTypeSlots: `
    DELETE FROM job_type_slots
    WHERE slot_count <= 0
  `,

  deleteWorkerJobTypeSlots: `
    DELETE FROM job_type_slots
    WHERE worker = ?
  `,
};
