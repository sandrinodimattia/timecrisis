import { Database } from 'better-sqlite3';

export function up(db: Database): void {
  db.exec(`
    CREATE TABLE IF NOT EXISTS jobs (
      id TEXT PRIMARY KEY,
      scheduled_job_id TEXT,
      reference_id TEXT,
      status TEXT NOT NULL CHECK (status IN ('pending', 'running', 'completed', 'failed', 'canceled')),
      type TEXT NOT NULL,
      data TEXT NOT NULL,
      priority INTEGER NOT NULL DEFAULT 10 CHECK (priority BETWEEN 1 AND 100),
      max_retries INTEGER NOT NULL DEFAULT 0,
      backoff_strategy TEXT NOT NULL DEFAULT 'exponential' 
        CHECK (backoff_strategy IN ('exponential', 'linear')),
      fail_reason TEXT,
      fail_count INTEGER NOT NULL DEFAULT 0,
      run_at TEXT,
      expires_at TEXT,
      started_at TEXT,
      finished_at TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      FOREIGN KEY (scheduled_job_id) REFERENCES scheduled_jobs(id)
    )
  `);

  db.exec(`
    CREATE TABLE IF NOT EXISTS job_runs (
      id TEXT PRIMARY KEY,
      job_id TEXT NOT NULL,
      status TEXT NOT NULL CHECK (status IN ('running', 'completed', 'failed')),
      started_at TEXT,
      finished_at TEXT,
      attempt INTEGER NOT NULL DEFAULT 1,
      progress INTEGER NOT NULL DEFAULT 0 CHECK (progress BETWEEN 0 AND 100),
      execution_duration INTEGER,
      error TEXT,
      error_stack TEXT,
      FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
    )
  `);

  db.exec(`
    CREATE TABLE IF NOT EXISTS job_logs (
      id TEXT PRIMARY KEY,
      job_id TEXT NOT NULL,
      job_run_id TEXT,
      level TEXT NOT NULL CHECK (level IN ('debug', 'info', 'warn', 'error')),
      timestamp TEXT NOT NULL,
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
      time_zone TEXT,
      data TEXT NOT NULL,
      last_scheduled_at TEXT,
      next_run_at TEXT,
      enabled INTEGER NOT NULL DEFAULT 1, 
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      UNIQUE(name, type)
    )
  `);

  db.exec(`
    CREATE TABLE IF NOT EXISTS dead_letter_jobs (
      id TEXT PRIMARY KEY,
      job_id TEXT NOT NULL,
      job_type TEXT NOT NULL,
      data TEXT,
      failed_at TEXT NOT NULL,
      failed_reason TEXT NOT NULL
    )
  `);

  db.exec(`
    CREATE TABLE IF NOT EXISTS distributed_locks (
      id TEXT PRIMARY KEY,
      worker TEXT NOT NULL,
      acquired_at TEXT NOT NULL,
      expires_at TEXT NOT NULL,
      created_at TEXT NOT NULL
    )
  `);

  db.exec(`
    CREATE TABLE IF NOT EXISTS workers (
      name TEXT PRIMARY KEY,
      first_seen TEXT NOT NULL,
      last_heartbeat TEXT NOT NULL
    )
  `);

  db.exec(`
    CREATE TABLE IF NOT EXISTS job_type_slots (
      job_type TEXT NOT NULL,
      worker TEXT NOT NULL,
      slot_count INTEGER NOT NULL DEFAULT 0,
      PRIMARY KEY (job_type, worker)
    )
  `);

  db.exec(`
    CREATE INDEX idx_jobs_reference ON jobs(reference_id);
    CREATE INDEX idx_jobs_reference_type ON jobs(reference_id, type);
    CREATE INDEX idx_jobs_type ON jobs(type);
    CREATE INDEX idx_jobs_status ON jobs(status);
    CREATE INDEX idx_jobs_expires ON jobs(expires_at);
    CREATE INDEX idx_jobs_status_priority ON jobs(status, priority);
    CREATE INDEX idx_logs_job ON job_logs(job_id);
    CREATE INDEX idx_logs_run ON job_logs(job_run_id);
    CREATE INDEX idx_runs_job ON job_runs(job_id);
    CREATE INDEX idx_job_runs_status ON job_runs(status);
    CREATE INDEX idx_locks_worker ON distributed_locks(worker);
    CREATE INDEX idx_locks_expires ON distributed_locks(expires_at);
    CREATE INDEX idx_dead_letter_job ON dead_letter_jobs(job_id);
    CREATE INDEX idx_scheduled_enabled ON scheduled_jobs(enabled);
    CREATE INDEX idx_scheduled_next ON scheduled_jobs(next_run_at);
  `);
}

export function down(db: Database): void {
  db.exec(`
    DROP TABLE IF EXISTS job_type_slots;
    DROP TABLE IF EXISTS workers;
    DROP TABLE IF EXISTS distributed_locks;
    DROP TABLE IF EXISTS dead_letter_jobs;
    DROP TABLE IF EXISTS scheduled_jobs;
    DROP TABLE IF EXISTS job_logs;
    DROP TABLE IF EXISTS job_runs;
    DROP TABLE IF EXISTS jobs;
  `);
}
