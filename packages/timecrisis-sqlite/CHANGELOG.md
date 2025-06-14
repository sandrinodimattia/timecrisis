# Time Crisis SQLite Storage Adapter

## 0.13.0

### Minor Changes

- 813a3ac: feat: allow specifying the lockTTL on a job definition level
- 8f6af90: feat: expire running jobs that haven't been touched in an hour
- a242fd4: feat: support expiration at the job definition level

### Patch Changes

- Updated dependencies [813a3ac]
- Updated dependencies [8f6af90]
- Updated dependencies [a242fd4]
  - @timecrisis/timecrisis@0.13.0

## 0.12.0

### Minor Changes

- 037e085: feat: track the touched_at on job runs

### Patch Changes

- Updated dependencies [037e085]
  - @timecrisis/timecrisis@0.12.0

## 0.11.0

### Minor Changes

- 63f0330: feat: get job settings from definition if not provided when enqueueing job

### Patch Changes

- Updated dependencies [63f0330]
  - @timecrisis/timecrisis@0.11.0

## 0.10.0

### Minor Changes

- 769af3a: feat: cascade down the reference ID to scheduled jobs

### Patch Changes

- Updated dependencies [769af3a]
  - @timecrisis/timecrisis@0.10.0

## 0.9.0

### Minor Changes

- 1e88589: chore: rename entity_id to reference_id

### Patch Changes

- Updated dependencies [1e88589]
  - @timecrisis/timecrisis@0.9.0

## 0.8.0

### Minor Changes

- 6386772: fix: make sure timezone is persisted in scheduled job

### Patch Changes

- Updated dependencies [6386772]
  - @timecrisis/timecrisis@0.8.0

## 0.7.0

### Minor Changes

- b57bb44: feat: make sure no data is lost for scheduled jobs when restarting the process

### Patch Changes

- Updated dependencies [b57bb44]
  - @timecrisis/timecrisis@0.7.0

## 0.6.0

### Minor Changes

- 388d762: feat: track the scheduled_job_id when a scheduled job creates a job
- ca133f3: when scheduling a job, upsert the job by name+type

### Patch Changes

- Updated dependencies [388d762]
- Updated dependencies [ca133f3]
  - @timecrisis/timecrisis@0.6.0

## 0.5.0

### Minor Changes

- cron jobs should only run once within a matched cron expression

### Patch Changes

- Updated dependencies
  - @timecrisis/timecrisis@0.5.0

## 0.4.0

### Minor Changes

- Allow reusing the same worker name in sqlite

### Patch Changes

- Updated dependencies
  - @timecrisis/timecrisis@0.4.0

## 0.3.0

### Minor Changes

- Major changes to the timecrisis libraries

### Patch Changes

- Updated dependencies
  - @timecrisis/timecrisis@0.3.0

## 0.2.0

### Minor Changes

- 39cb889: Move to scoped packages

### Patch Changes

- Updated dependencies [39cb889]
  - @timecrisis/timecrisis@0.2.0

All notable changes to this project will be documented in this file.

## 0.1.1

### Patch Changes

- README improvements
- Updated dependencies
  - timecrisis@0.1.1
