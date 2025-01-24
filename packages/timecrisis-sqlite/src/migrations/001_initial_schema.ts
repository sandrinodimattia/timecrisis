import { Database } from 'better-sqlite3';

export function up(db: Database): void {
  db.exec(`
    CREATE TABLE IF NOT EXISTS jobs (
      id TEXT PRIMARY KEY,
      type TEXT NOT NULL,
      status TEXT NOT NULL CHECK (status IN ('pending', 'running', 'completed', 'failed', 'scheduled')),
      data TEXT NOT NULL,
      priority INTEGER NOT NULL DEFAULT 1,
      progress INTEGER NOT NULL DEFAULT 0,
      attempts INTEGER NOT NULL DEFAULT 0,
      max_retries INTEGER NOT NULL DEFAULT 0,
      backoff_strategy TEXT NOT NULL DEFAULT 'exponential' 
        CHECK (backoff_strategy IN ('exponential', 'linear')),
      fail_reason TEXT,
      fail_count INTEGER NOT NULL DEFAULT 0,
      execution_duration INTEGER,
      reference_id TEXT,
      expires_at TEXT,
      locked_at TEXT,
      started_at TEXT,
      run_at TEXT,
      finished_at TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    )
  `);

  db.exec(`
    CREATE TABLE IF NOT EXISTS job_runs (
      id TEXT PRIMARY KEY,
      job_id TEXT NOT NULL,
      status TEXT NOT NULL CHECK (status IN ('running', 'completed', 'failed')),
      started_at TEXT,
      progress INTEGER NOT NULL DEFAULT 0,
      finished_at TEXT,
      error TEXT,
      error_stack TEXT,
      attempt INTEGER NOT NULL DEFAULT 1,
      FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
    )
  `);

  db.exec(`
    CREATE TABLE IF NOT EXISTS job_logs (
      id TEXT PRIMARY KEY,
      job_id TEXT NOT NULL,
      job_run_id TEXT,
      timestamp TEXT NOT NULL,
      level TEXT NOT NULL CHECK (level IN ('debug', 'info', 'warn', 'error')),
      message TEXT NOT NULL,
      metadata TEXT,
      FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE,
      FOREIGN KEY (job_run_id) REFERENCES job_runs(id) ON DELETE CASCADE
    )
  `);

  db.exec(`
    CREATE TABLE IF NOT EXISTS scheduled_jobs (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      type TEXT NOT NULL,
      schedule_type TEXT NOT NULL 
        CHECK (schedule_type IN ('exact', 'interval', 'cron')),
      schedule_value TEXT NOT NULL,
      data TEXT NOT NULL,
      enabled INTEGER NOT NULL DEFAULT 1, 
        -- store booleans as 0/1
      last_scheduled_at TEXT,
      next_run_at TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    )
  `);

  db.exec(`
    CREATE TABLE IF NOT EXISTS dead_letter_jobs (
      id TEXT PRIMARY KEY,
      job_id TEXT NOT NULL,
      job_type TEXT NOT NULL,
      data TEXT,
      failed_at TEXT NOT NULL,
      reason TEXT NOT NULL
    )
  `);

  db.exec(`
    CREATE TABLE IF NOT EXISTS distributed_locks (
      id TEXT PRIMARY KEY,
      owner TEXT NOT NULL UNIQUE,
      expires_at TEXT NOT NULL,
      acquired_at TEXT NOT NULL,
      created_at TEXT NOT NULL
    )
  `);

  db.exec(`
    CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
    CREATE INDEX IF NOT EXISTS idx_jobs_type ON jobs(type);
    CREATE INDEX IF NOT EXISTS idx_jobs_reference ON jobs(reference_id);
    CREATE INDEX IF NOT EXISTS idx_jobs_locked ON jobs(locked_at);
    CREATE INDEX IF NOT EXISTS idx_jobs_expires ON jobs(expires_at);
    CREATE INDEX IF NOT EXISTS idx_runs_job ON job_runs(job_id);
    CREATE INDEX IF NOT EXISTS idx_logs_job ON job_logs(job_id);
    CREATE INDEX IF NOT EXISTS idx_logs_run ON job_logs(job_run_id);
    CREATE INDEX IF NOT EXISTS idx_scheduled_next ON scheduled_jobs(next_run_at);
    CREATE INDEX IF NOT EXISTS idx_scheduled_enabled ON scheduled_jobs(enabled);
    CREATE INDEX IF NOT EXISTS idx_dead_letter_job ON dead_letter_jobs(job_id);
    CREATE INDEX IF NOT EXISTS idx_locks_expires ON distributed_locks(expires_at);
    CREATE INDEX IF NOT EXISTS idx_locks_owner ON distributed_locks(owner);
  `);
}

export function down(db: Database): void {
  db.exec(`
    DROP TABLE IF EXISTS distributed_locks;
    DROP TABLE IF EXISTS dead_letter_jobs;
    DROP TABLE IF EXISTS scheduled_jobs;
    DROP TABLE IF EXISTS job_logs;
    DROP TABLE IF EXISTS job_runs;
    DROP TABLE IF EXISTS jobs;
  `);
}
