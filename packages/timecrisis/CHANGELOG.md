# Time Crisis Job Scheduler

## 0.17.0

### Minor Changes

- c2f32b7: fix: make sure there is a worker registration when the leadership is acquired

## 0.16.0

### Minor Changes

- 383468e: fix: validate data when persisting updates in a job

## 0.15.0

### Minor Changes

- 8a25b38: feat: expose logger on job context

## 0.14.0

### Minor Changes

- d14c759: feat: use the job name as a child logger

## 0.13.0

### Minor Changes

- 813a3ac: feat: allow specifying the lockTTL on a job definition level
- 8f6af90: feat: expire running jobs that haven't been touched in an hour
- a242fd4: feat: support expiration at the job definition level

## 0.12.0

### Minor Changes

- 037e085: feat: track the touched_at on job runs

## 0.11.0

### Minor Changes

- 63f0330: feat: get job settings from definition if not provided when enqueueing job

## 0.10.0

### Minor Changes

- 769af3a: feat: cascade down the reference ID to scheduled jobs

## 0.9.0

### Minor Changes

- 1e88589: chore: rename entity_id to reference_id

## 0.8.0

### Minor Changes

- 6386772: fix: make sure timezone is persisted in scheduled job

## 0.7.0

### Minor Changes

- b57bb44: feat: make sure no data is lost for scheduled jobs when restarting the process

## 0.6.0

### Minor Changes

- 388d762: feat: track the scheduled_job_id when a scheduled job creates a job
- ca133f3: when scheduling a job, upsert the job by name+type

## 0.5.0

### Minor Changes

- cron jobs should only run once within a matched cron expression

## 0.4.0

### Minor Changes

- Allow reusing the same worker name in sqlite

## 0.3.0

### Minor Changes

- Major changes to the timecrisis libraries

## 0.2.0

### Minor Changes

- 39cb889: Move to scoped packages

All notable changes to this project will be documented in this file.

## 0.1.1

### Patch Changes

- README improvements
