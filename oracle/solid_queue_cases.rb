# Oracle cases for the SolidQueue-style PG queue (Iteration 7).
#
# Run against the ORIGINAL gems (solid_queue 1.2.4 + fugit 1.12.1):
#   TZ=UTC ruby oracle/solid_queue_cases.rb > oracle/solid_queue_expected.yml
#
# Each line: "<case_id>\t<value>". The same case IDs are mirrored 1:1 in
# spec/solid_queue_spec.cr; the Crystal port must reproduce the file
# exactly. Cases capture the OBSERVABLE queue semantics (enqueue rows,
# claim ordering with SKIP LOCKED, scheduled-at gating, recurring cron
# math), not solid_queue's internal schema.
#
# Requires local PostgreSQL; the script creates/drops the database
# crails_sq_oracle itself.

ENV["TZ"] = "UTC"

gem "activerecord", "= 8.1.4"
gem "activejob", "= 8.1.4"
gem "activesupport", "= 8.1.4"
gem "solid_queue", "= 1.2.4"
gem "rails", "= 8.1.4"
gem "pg"
gem "fugit"

require "active_job"
require "active_record"
require "rails"
require "solid_queue"
require "fugit"

# solid_queue's app/models are wired through the Rails engine/Zeitwerk;
# standalone we register a loader for the gem's app/models, exactly what
# the engine's isolate_namespace does.
SQ_GEM = Gem::Specification.find_by_name("solid_queue").gem_dir
require "zeitwerk"
sq_loader = Zeitwerk::Loader.new
sq_loader.push_dir(File.join(SQ_GEM, "app/models"))
sq_loader.push_dir(File.join(SQ_GEM, "app/jobs"))
sq_loader.setup

# The engine normally includes this into ActiveJob::Base.
require "active_job/concurrency_controls"
ActiveJob::Base.include ActiveJob::ConcurrencyControls
# standalone core_ext bits the engine normally loads
require "active_support/core_ext/string/starts_ends_with"

ActiveJob::Base.logger = Logger.new(File::NULL)
SolidQueue.logger = Logger.new(File::NULL)

DB = "crails_sq_oracle"
`dropdb --if-exists #{DB}`
`createdb #{DB}` or abort "createdb #{DB} failed"

ActiveRecord::Base.establish_connection(
  adapter: "postgresql", database: DB, pool: 5
)
ActiveRecord::Base.connection.drop_table "schema_migrations", if_exists: true
schema = File.read(File.expand_path("vendor/solid_queue_schema.rb", __dir__))
eval(schema, TOPLEVEL_BINDING, "solid_queue_schema.rb")

def render(v)
  v.is_a?(String) ? v : v.inspect
end

def case_run(id)
  # each case starts from a clean queue
  [ SolidQueue::ClaimedExecution, SolidQueue::ReadyExecution,
    SolidQueue::ScheduledExecution, SolidQueue::FailedExecution,
    SolidQueue::RecurringExecution, SolidQueue::Job,
    SolidQueue::Process, SolidQueue::Pause ].each(&:delete_all)
  LOG.clear
  puts "#{id}\t#{render(yield)}"
rescue StandardError => e
  warn "=== #{id}: #{e.class}: #{e.message}"
  warn e.backtrace.select { |l| l.include?("solid_queue-1.2.4") || l.include?("solid_queue_cases") }.first(6)
  puts "#{id}\traise:#{e.class.name}"
end

LOG = []

class A < ActiveJob::Base
  def perform(x = nil); LOG << "A:#{x}"; "A"; end
end
class B < ActiveJob::Base
  def perform(x = nil); LOG << "B:#{x}"; "B"; end
end
class C < ActiveJob::Base
  queue_as :low
  def perform(x = nil); LOG << "C:#{x}"; "C"; end
end
class PrioA < ActiveJob::Base
  queue_with_priority 5
  def perform(x = nil); LOG << "PrioA:#{x}"; end
end
class PrioB < ActiveJob::Base
  queue_with_priority 1
  def perform(x = nil); LOG << "PrioB:#{x}"; end
end
class PrioC < ActiveJob::Base
  queue_with_priority 1
  def perform(x = nil); LOG << "PrioC:#{x}"; end
end
class Boom < StandardError; end
class Flaky < ActiveJob::Base
  retry_on Boom, attempts: 2, wait: 0
  def perform(x = nil)
    LOG << "Flaky:#{x}"
    raise Boom unless LOG.count("Flaky:#{x}") > 1
    "ok"
  end
end

ActiveJob::Base.queue_adapter = :solid_queue

# Deterministic rendering of a job row: volatile fields (job_id uuid,
# enqueued_at, "scheduled_at == now" for immediate enqueues) stripped.
def row(j)
  args = j.arguments.dup
  if args.is_a?(Hash)
    %w[job_id enqueued_at scheduled_at provider_job_id].each { |k| args.delete(k) }
    args = args.to_a.sort.map { |k, v| "#{k}=#{v.inspect}" }.join(" ")
  end
  sched = if j.scheduled_at.nil?
    "nil"
  elsif j.scheduled_at > Time.now.utc + 1.hour
    j.scheduled_at.utc.iso8601
  else
    "now"
  end
  "#{j.queue_name}|#{j.class_name}|#{j.priority}|#{args}|sched:#{sched}|fin:#{!!j.finished_at}"
end

def job_rows(active_job_id: nil)
  scope = SolidQueue::Job.all
  scope = scope.where(active_job_id: active_job_id) if active_job_id
  scope.order(:id).map { |j| row(j) }.join("\n")
end

def make_process(name)
  SolidQueue::Process.create!(kind: "Worker", name: name,
                              pid: Process.pid, last_heartbeat_at: Time.now.utc)
end

FIXED_NOW = Time.utc(2026, 10, 3, 12, 34, 56)

# --- enqueue row semantics --------------------------------------------

case_run "sq_enqueue_rows" do
  A.perform_later(1)
  C.perform_later("x")
  B.perform_later
  job_rows
end

case_run "sq_scheduled_at" do
  B.set(wait_until: Time.utc(2031, 1, 1, 0, 0, 0)).perform_later
  job_rows
end

# --- claim ordering (priority ASC, job_id ASC = FIFO) ------------------

case_run "sq_claim_order" do
  PrioA.perform_later   # priority 5 — last
  PrioB.perform_later   # priority 1 — second
  PrioC.perform_later   # priority 1 — third (FIFO after PrioB)
  A.perform_later       # priority 0 — first
  p1 = make_process("w1")
  claimed = SolidQueue::ReadyExecution.claim(["default"], 10, p1.id)
  claimed.map { |c| c.job.class_name }.join(",")
end

case_run "sq_claim_two_workers" do
  4.times { |i| B.perform_later(i) }
  p1 = make_process("w1")
  p2 = make_process("w2")
  c1 = SolidQueue::ReadyExecution.claim(["default"], 2, p1.id).map(&:job_id).sort
  c2 = SolidQueue::ReadyExecution.claim(["default"], 2, p2.id).map(&:job_id).sort
  "#{c1.join(",")} #{c2.join(",")} disjoint:#{(c1 & c2).empty?}"
end

case_run "sq_claim_queuelist" do
  C.perform_later(1)
  A.perform_later(1)
  p1 = make_process("w1")
  claimed = SolidQueue::ReadyExecution.claim(%w[low default], 10, p1.id)
  claimed.map { |c| "#{c.job.queue_name}:#{c.job.class_name}" }.sort.join(",")
end

# --- scheduled gating ----------------------------------------------------

case_run "sq_scheduled_not_ready" do
  B.set(wait_until: Time.utc(2031, 1, 1)).perform_later
  p1 = make_process("w1")
  c = SolidQueue::ReadyExecution.claim(["default"], 10, p1.id)
  "claimed:#{c.size} scheduled:#{SolidQueue::ScheduledExecution.count}"
end

case_run "sq_scheduled_dispatch_due" do
  B.set(wait_until: Time.utc(2031, 1, 1)).perform_later
  # backdate: the job is now due
  SolidQueue::ScheduledExecution.update_all("scheduled_at = now() - interval '1 second'")
  n = SolidQueue::ScheduledExecution.dispatch_next_batch(10)
  p1 = make_process("w1")
  c = SolidQueue::ReadyExecution.claim(["default"], 10, p1.id)
  "dispatched:#{n} claimed:#{c.map(&:job_id).size}"
end

case_run "sq_scheduled_order" do
  B.set(wait_until: Time.utc(2031, 1, 2)).perform_later # later
  B.set(wait_until: Time.utc(2031, 1, 1)).perform_later # earlier
  rows = SolidQueue::ScheduledExecution.order(scheduled_at: :asc, priority: :asc, job_id: :asc).limit(2)
    .map { |e| e.job.scheduled_at.utc.iso8601 }
  rows.join(",")
end

# --- perform path --------------------------------------------------------

case_run "sq_perform_result" do
  A.perform_later(7)
  p1 = make_process("w1")
  claimed = SolidQueue::ReadyExecution.claim(["default"], 1, p1.id)
  execution = claimed.first
  result = execution.perform
  "ok:#{!result.nil?} finished:#{execution.job.reload.finished_at.present?} claimed_left:#{SolidQueue::ClaimedExecution.count} log:#{LOG.last}"
end

case_run "sq_failure_and_retry" do
  Flaky.perform_later(9)
  p1 = make_process("w1")
  claimed = SolidQueue::ReadyExecution.claim(["default"], 1, p1.id)
  outcome = begin
    claimed.first.perform
    "performed"
  rescue Boom
    "raised:Boom"
  end
  # ActiveJob retry_on re-enqueued (wait: 0) — dispatch, claim, succeed.
  SolidQueue::ScheduledExecution.dispatch_next_batch(10)
  c2 = SolidQueue::ReadyExecution.claim(["default"], 1, p1.id)
  c2.first.perform
  "#{outcome} attempts:#{LOG.count("Flaky:9")} flaky_rows:#{SolidQueue::Job.where(class_name: "Flaky").count} finished:#{SolidQueue::Job.where(class_name: "Flaky", finished_at: nil).count == 0}"
end

# --- recurring (cron math via fugit + task enqueue) ----------------------

CRONS = [
  "* * * * *",
  "*/5 * * * *",
  "0 12 * * 1",
  "30 2 1 * *",
  "0 9-17/2 * * mon-fri",
  "5 4 * * sun",
  "@daily",
  "@hourly",
  "bad cron",
]

case_run "sq_cron_next" do
  CRONS.map { |c|
    f = Fugit::Cron.parse(c)
    f ? f.next_time(FIXED_NOW).utc.iso8601 : "nil"
  }.join(",")
end

case_run "sq_cron_prev" do
  CRONS.map { |c|
    f = Fugit::Cron.parse(c)
    f ? f.previous_time(FIXED_NOW).utc.iso8601 : "nil"
  }.join(",")
end

case_run "sq_recurring_task_enqueue" do
  task = SolidQueue::RecurringTask.from_configuration(
    :cache_sweeper, class: "A", schedule: "@daily", queue: "low", args: [1, "x"], priority: 3
  )
  at = Time.utc(2031, 1, 1, 0, 0, 0)
  task.enqueue(at: at)
  j = SolidQueue::Job.order(:id).last
  run = SolidQueue::RecurringExecution.last
  "#{row(j)}|run_at:#{run.run_at.utc.iso8601}|task:#{run.task_key}|count:#{SolidQueue::RecurringExecution.where(task_key: "cache_sweeper").size}"
end

ActiveRecord::Base.connection_pool.disconnect
`dropdb --if-exists #{DB}`
