# active_job.cr — порт ActiveJob

> ⚠️ EXPERIMENTAL FRAMEWORK, DO NOT USE IN PRODUCTION YET.

Неймспейс: `ActiveJob` — 1:1. Оценка: ~90% (меты почти ноль; один шов —
десериализация класса джобы → compile-time registry).

## API 1:1

### DSL
- `class ProcessJob < ApplicationJob` (`< ActiveJob::Base`),
  `def perform(*args)`
- `queue_as :default` / `queue_as do ... end`; `queue_with_priority N`
- `retry_on Error, wait: 3.seconds, attempts: 5, jitter: 0.15, queue:,
  priority:` (waits: polynomial `:polynomially_longer`; exhausted-блок)
- `discard_on Error`; `after_discard do |job, e| end`
- Коллбэки: `before/after/around_perform`, `before/after/around_enqueue`
- `rescue_from`
- Continuable jobs (8.x): `include ActiveJob::Continuable`,
  `step :name, start:, isolated: do ... end`

### API вызова
- `ProcessJob.perform_later(*args)` (+ блок), `perform_now`,
  `set(queue:, wait:, wait_until:, priority:).perform_later`,
  `perform_all_later(*jobs)` (bulk)
- Инстанс: `retry_job(wait:, queue:)`, `job_id`, `queue_name`,
  `priority`, `arguments`, `executions`, `exception_executions`,
  `scheduled_at`, `provider_job_id`, `successfully_enqueued?`
- Сериализация аргументов: примитивы as-is; Date/Time/Duration/Range/
  NamedTuple через реестр сериалайзеров; **GlobalID** — AR-модель в
  аргументах → `gid://app/User/1`, резолв при выполнении, отсутствующая
  запись → `DeserializationError::RecordNotFound` (типично `discard_on`)
- Locale/timezone переносятся в джобу (I18n.with_locale-обёртка)

### Адаптеры
Интерфейс `enqueue(job) / enqueue_at(job, ts)`: `async` (по умолчанию,
fiber-пул), `inline`, `test`; внешние: PG-очередь (`active_job.cr/adapters/pg`,
skip-locked — кандидат №1 для монолита), Redis-очередь.
`config.active_job.queue_adapter`, per-class `queue_adapter=`.

## Реализация

- `async`-адаптер: очередь `Channel(Job)` + fiber-пул воркеров;
  scheduled-джобы — timer heap (fiber sleep).
- Десериализация `job_class` по строке: compile-time registry
  (`{{ @type.all_subclasses }}` от `ActiveJob::Base`) — все джобы
  известны на компиляции; GlobalID-локатор — тот же registry AR-классов.
- Retry-математика (polynomial backoff `executions**4 + rand*jitter + 2`)
  — статика, 1:1.
- Метрики: `enqueue.active_job` / `perform.active_job` через
  ActiveSupport::Notifications — для bench/.

## Фазы

- P0: Base + perform_later/perform_now + async-адаптер + retry/discard +
  коллбэки + сериализация примитивов.
- P1: GlobalID, PG-адаптер (durable очередь для монолита), scheduled,
  bulk perform_all_later, Continuable.
- P2: test-helpers (`assert_enqueued_with`, perform_enqueued_jobs),
  Redis-адаптер, enqueue_after_transaction_commit.

## Статус (Iteration 7 — PG-очередь + recurring + Redis-очередь)

- `src/solid_queue.cr` — `ActiveJob::SolidQueue::Adapter`: durable
  PG-очередь поверх порта `Adapter` (одноимённого модуля). Таблица
  `solid_queue_jobs` + claimed_at/finished_at; клейм —
  `FOR UPDATE SKIP LOCKED`, порядок priority DESC (выше число — раньше,
  снято с реального solid_queue), FIFO по id; scheduled-джобы невидимы
  до scheduled_at; retry — на уровне ActiveJob (новая строка);
  stale-claim reclaim (`release_stale_claims`, дефолт 1 ч).
- Recurring: `src/cron.cr` (`ActiveJob::Cron`, семантика fugit —
  next_time строго после, previous_time не позже, DOM/DOW-union,
  `@daily`-алиасы) + `SolidQueue::RecurringTask` (key/class/schedule/
  queue/priority/args), дедуп `(task_key, run_at)` частичным уникальным
  индексом; `Adapter#schedule_recurring` — тик шедулера.
- `src/adapters/redis_queue.cr` — `RedisQueueAdapter`: LIST на очередь
  (LPUSH/RPOP), ZSET для scheduled, recurring-дедуп SET NX; эфемерный
  (без истории, без приоритетов внутри очереди — зафиксировано).
- `retry_on` теперь принимает `wait:` (дефолт 3.seconds, как Rails);
  `Base.find_job_class` — lookup класса в compile-time registry.
- Оракул: `oracle/solid_queue_cases.rb` → `solid_queue_expected.yml`
  (15 кейсов), снят с настоящего solid_queue 1.2.4 + fugit 1.12.1
  (standalone-загрузка гема через Zeitwerk); vendor-схема в
  `oracle/vendor/`. Зеркало `spec/solid_queue_spec.cr` (те же ID,
  без правок) + юнит-кроны за пределами оракульной строки.
  `spec/redis_queue_spec.cr` — контракт Redis-очереди. Сюит 56/56.
- Задокументированные отклонения от solid_queue: одна таблица вместо
  ready/claimed/scheduled-сплита; нет concurrency controls / пауз /
  process heartbeats (P2); recurring-строка получает scheduled_at=now
  (шедулер энкьюит в момент наступления, run_at хранит вхождение).
- Бенч: `bench/crails_solidqueue` → `bench/results/201-solid-queue.md`.

## Готово, когда

джоба с `retry_on` ретраится с backoff после падения, аргумент-модель
резолвится через GlobalID, а `deliver_later` из action_mailer.cr
работает через эту же очередь.

## Зависимости

active_support.cr, active_record.cr (GlobalID), crystal-pg.
