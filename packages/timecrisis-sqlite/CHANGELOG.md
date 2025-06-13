# Time Crisis SQLite Storage Adapter

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
