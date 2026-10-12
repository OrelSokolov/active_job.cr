# active_job.cr

A Crystal port of Rails **ActiveJob** — background jobs with retries,
callbacks, serialization and pluggable queue adapters, plus a durable
PostgreSQL queue modeled on **solid_queue 1.2.4** and a Redis queue.
Namespace `ActiveJob` mirrors the original 1:1. Estimated parity ~90%;
the single structural seam is job-class deserialization, solved with a
compile-time registry instead of Ruby's `constantize`.

> ⚠️ EXPERIMENTAL FRAMEWORK, DO NOT USE IN PRODUCTION YET.

## Installation

```yaml
dependencies:
  active_job_cr:
    github: OrelSokolov/active_job.cr
```

```sh
shards install
```

Requires Crystal >= 1.21.0; depends on `active_support.cr`,
`active_record.cr` (GlobalID) and crystal-pg for the SolidQueue adapter.

## Usage

```crystal
class ProcessJob < ActiveJob::Base
  queue_as :default

  retry_on MyError, wait: 3.seconds, attempts: 5, jitter: 0.15
  discard_on ActiveJob::DeserializationError::RecordNotFound

  def perform(user : User, priority : Int32)
    # ...
  end
end

ProcessJob.perform_later(user, 1)
ProcessJob.set(wait: 10.minutes, queue: :low).perform_later(user, 2)
```

## Feature comparison vs ActiveJob (Rails 8.x)

### DSL

| Feature | Original (activejob) | Port (active_job.cr) | Status |
|---|---|---|---|
| `class XJob < ActiveJob::Base`, `def perform(*args)` | ✅ | Same | ✅ |
| `queue_as :name` / block form; `queue_with_priority N` | ✅ | Same | ✅ |
| `retry_on Error, wait:, attempts:, jitter:, queue:, priority:, waits: :polynomially_longer` (+ exhausted block) | ✅ | Same retry math (polynomial backoff `executions**4 + rand*jitter + 2`) | ✅ |
| `discard_on Error`, `after_discard` | ✅ | Same | ✅ |
| Callbacks: `before/after/around_perform`, `before/after/around_enqueue` | ✅ | Same | ✅ |
| `rescue_from` | ✅ | Same | ✅ |
| Continuable jobs (`include ActiveJob::Continuable`, `step :name, start:, isolated:`) | ✅ | Same (8.x feature) | ✅ |

### Invocation & serialization

| Feature | Original | Port | Status |
|---|---|---|---|
| `perform_later` (+ block), `perform_now`, `set(queue:, wait:, wait_until:, priority:)`, `perform_all_later` (bulk) | ✅ | Same | ✅ |
| Instance API: `retry_job(wait:, queue:)`, `job_id`, `queue_name`, `priority`, `arguments`, `executions`, `exception_executions`, `scheduled_at`, `provider_job_id`, `successfully_enqueued?` | ✅ | Same | ✅ |
| Argument serialization: primitives as-is; Date/Time/Duration/Range/NamedTuple | ✅ | Via a serializer registry | ✅ |
| GlobalID (`gid://app/User/1`), missing record → `DeserializationError::RecordNotFound` | ✅ | Compile-time registry of AR classes instead of `constantize` | ✅ |
| Locale/timezone carried into the job (`I18n.with_locale` wrapper) | ✅ | Same | ✅ |

### Adapters

| Feature | Original | Port | Status |
|---|---|---|---|
| `async` adapter (default) | ✅ | `Channel(Job)` queue + fiber worker pool; scheduled jobs on a timer heap | ✅ |
| `inline`, `test` adapters | ✅ | Same interface (`enqueue`/`enqueue_at`) | ✅ |
| `config.active_job.queue_adapter`, per-class `queue_adapter=` | ✅ | Same | ✅ |
| solid_queue adapter | separate gem (8.x default) | `ActiveJob::SolidQueue::Adapter`: PG table `solid_queue_jobs`, `FOR UPDATE SKIP LOCKED` claims, priority DESC + FIFO by id, scheduled-job visibility, stale-claim reclaim (default 1 h) | 🔶 |
| Recurring jobs (fugit cron semantics, `@daily` aliases, DOM/DOW union) | ✅ | `ActiveJob::Cron` + `SolidQueue::RecurringTask` with `(task_key, run_at)` dedup via partial unique index | 🔶 |
| Redis adapter | Sidekiq etc. | `RedisQueueAdapter`: LIST queue (LPUSH/RPOP), ZSET for scheduled, SET NX recurring dedup; ephemeral (no history, no in-queue priorities — by design) | 🔶 |
| Test helpers (`assert_enqueued_with`, `perform_enqueued_jobs`) | ✅ | Not ported | ⬜ |
| `enqueue_after_transaction_commit` | ✅ | Not ported | ⬜ |

### Deviations from solid_queue (documented)

- One jobs table instead of the ready/claimed/scheduled split.
- No concurrency controls, pauses or process heartbeats.
- Recurring rows get `scheduled_at = now` (the scheduler enqueues at
  tick time; `run_at` stores the occurrence).

## Testing

```sh
crystal spec
```

Suite: 56/56. The SolidQueue oracle (`oracle/solid_queue_cases.rb` →
`solid_queue_expected.yml`, 15 cases) is recorded against the real
solid_queue 1.2.4 + fugit 1.12.1 and replayed unchanged in
`spec/solid_queue_spec.cr`; `spec/redis_queue_spec.cr` covers the Redis
queue contract. Benchmarks: `bench/results/201-solid-queue.md`.
