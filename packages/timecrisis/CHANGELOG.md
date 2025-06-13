# Time Crisis Job Scheduler

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
