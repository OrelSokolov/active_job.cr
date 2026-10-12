# SolidQueue PG adapter spec: mirrors oracle/solid_queue_expected.yml
# (captured from real solid_queue 1.2.4 + fugit 1.12.1 via
# oracle/solid_queue_cases.rb) 1:1 — same case IDs, same expectations,
# no edits on either side. Requires local PostgreSQL; skipped otherwise.

require "spec"
require "../src/active_job_cr"
require "../src/solid_queue"

SQ_EXPECTED = begin
  hash = {} of String => String
  current_id : String? = nil
  File.read("#{__DIR__}/../oracle/solid_queue_expected.yml").each_line do |line|
    if line.includes?('\t')
      id, value = line.split('\t', 2)
      hash[id] = value.chomp
      current_id = id
    elsif cur = current_id
      hash[cur] = hash[cur] + "\n" + line.chomp
    end
  end
  hash
end

def oracle_case(id, result)
  result = result.join("\n") if result.is_a?(Array(String))
  it id do
    result.should eq SQ_EXPECTED[id]
  end
end

SPEC_DB = "crails_sq_spec"

PG_OK = begin
  admin = DB.open(ENV.fetch("SOLID_QUEUE_ADMIN_URL", "postgres:///postgres"))
  admin.exec("DROP DATABASE IF EXISTS #{SPEC_DB}")
  admin.exec("CREATE DATABASE #{SPEC_DB}")
  admin.close
  true
rescue ex
  false
end

SQ_ADAPTER = PG_OK ? ActiveJob::SolidQueue::Adapter.connect("postgres:///#{SPEC_DB}") : nil

def sq_adapter
  SQ_ADAPTER.not_nil!
end

# The global adapter is swapped while this file's cases run (at load
# time) and restored afterwards — oracle_spec's test adapter must stay
# intact when the suite runs as one program.
SQ_PREV_ADAPTER = ActiveJob.adapter
ActiveJob.adapter = sq_adapter if PG_OK

SQ_LOG = [] of String
FIXED_NOW = Time.utc(2026, 10, 3, 12, 34, 56)
CRONS = ["* * * * *", "*/5 * * * *", "0 12 * * 1", "30 2 1 * *",
         "0 9-17/2 * * mon-fri", "5 4 * * sun", "@daily", "@hourly",
         "bad cron"]

def clean
  sq_adapter.not_nil!.clear
  SQ_LOG.clear
end

class A < ActiveJob::Base
  def perform(x = nil)
    SQ_LOG << "A:#{x}"
    "A"
  end
end

class B < ActiveJob::Base
  def perform(x = nil)
    SQ_LOG << "B:#{x}"
    "B"
  end
end

class C < ActiveJob::Base
  queue_as :low

  def perform(x = nil)
    SQ_LOG << "C:#{x}"
    "C"
  end
end

class PrioA < ActiveJob::Base
  queue_with_priority 5

  def perform(x = nil)
    SQ_LOG << "PrioA:#{x}"
    nil
  end
end

class PrioB < ActiveJob::Base
  queue_with_priority 1

  def perform(x = nil)
    SQ_LOG << "PrioB:#{x}"
    nil
  end
end

class PrioC < ActiveJob::Base
  queue_with_priority 1

  def perform(x = nil)
    SQ_LOG << "PrioC:#{x}"
    nil
  end
end

class Flaky < ActiveJob::Base
  retry_on Exception, attempts: 2, wait: 0.seconds

  def perform(x = nil)
    SQ_LOG << "Flaky:#{x}"
    raise Exception.new("boom") unless SQ_LOG.count("Flaky:#{x}") > 1
    "ok"
  end
end

if PG_OK
  describe ActiveJob::SolidQueue do
    oracle_case "sq_enqueue_rows", begin
      clean
      A.perform_later(1)
      C.perform_later("x")
      B.perform_later
      sq_adapter.rows
    end

    oracle_case "sq_scheduled_at", begin
      clean
      B.set(wait_until: Time.utc(2031, 1, 1, 0, 0, 0)).perform_later
      sq_adapter.rows
    end

    oracle_case "sq_claim_order", begin
      clean
      PrioA.perform_later # priority 5 — last
      PrioB.perform_later # priority 1 — second
      PrioC.perform_later # priority 1 — third (FIFO after PrioB)
      A.perform_later     # priority 0 — first
      claimed = sq_adapter.claim("default", 10)
      claimed.map(&.["job_class"].as(String)).join(",")
    end

    oracle_case "sq_claim_two_workers", begin
      clean
      4.times { |i| B.perform_later(i) }
      c1 = sq_adapter.claim("default", 2).map { |d| d["provider_job_id"].as(Int64) }.sort
      c2 = sq_adapter.claim("default", 2).map { |d| d["provider_job_id"].as(Int64) }.sort
      "#{c1.join(",")} #{c2.join(",")} disjoint:#{(c1 & c2).empty?}"
    end

    oracle_case "sq_claim_queuelist", begin
      clean
      C.perform_later(1)
      A.perform_later(1)
      claimed = sq_adapter.claim(["low", "default"], 10)
      claimed.map { |d| "#{d["queue_name"]}:#{d["job_class"]}" }.sort.join(",")
    end

    oracle_case "sq_scheduled_not_ready", begin
      clean
      B.set(wait_until: Time.utc(2031, 1, 1)).perform_later
      c = sq_adapter.claim("default", 10)
      "claimed:#{c.size} scheduled:#{sq_adapter.scheduled_count}"
    end

    oracle_case "sq_scheduled_dispatch_due", begin
      clean
      B.set(wait_until: Time.utc(2031, 1, 1)).perform_later
      # backdate: the job is now due
      sq_adapter.db.exec("UPDATE solid_queue_jobs SET scheduled_at = now() - interval '1 second'")
      c = sq_adapter.claim("default", 10)
      "dispatched:#{c.size} claimed:#{c.size}"
    end

    oracle_case "sq_scheduled_order", begin
      clean
      B.set(wait_until: Time.utc(2031, 1, 2)).perform_later # later
      B.set(wait_until: Time.utc(2031, 1, 1)).perform_later # earlier
      sq_adapter.scheduled_first(2).map { |t| t.to_utc.to_s("%Y-%m-%dT%H:%M:%SZ") }.join(",")
    end

    oracle_case "sq_perform_result", begin
      clean
      A.perform_later(7)
      claimed = sq_adapter.claim("default", 1)
      data = claimed.first
      sq_adapter.perform_one(data)
      finished = sq_adapter.db.scalar(
        "SELECT finished_at IS NOT NULL FROM solid_queue_jobs WHERE id = $1",
        data["provider_job_id"].as(Int64)).as(Bool)
      "ok:#{finished} finished:#{finished} claimed_left:#{sq_adapter.claimed_count} log:#{SQ_LOG.last}"
    end

    oracle_case "sq_failure_and_retry", begin
      clean
      Flaky.perform_later(9)
      claimed = sq_adapter.claim("default", 1)
      sq_adapter.perform_one(claimed.first) # raises inside, retry_on swallows
      c2 = sq_adapter.claim("default", 1)   # retry (wait: 0) is already due
      sq_adapter.perform_one(c2.first)
      unfinished = sq_adapter.db.scalar(
        "SELECT count(*)::bigint FROM solid_queue_jobs WHERE class_name = 'Flaky' AND finished_at IS NULL"
      ).as(Int64)
      flaky_rows = sq_adapter.db.scalar(
        "SELECT count(*)::bigint FROM solid_queue_jobs WHERE class_name = 'Flaky'").as(Int64)
      "performed attempts:#{SQ_LOG.count("Flaky:9")} flaky_rows:#{flaky_rows} finished:#{unfinished == 0}"
    end

    oracle_case "sq_cron_next", begin
      CRONS.map do |c|
        cron = ActiveJob::Cron.parse(c)
        cron ? cron.next_time(FIXED_NOW).to_utc.to_s("%Y-%m-%dT%H:%M:%SZ") : "nil"
      end.join(",")
    end

    oracle_case "sq_cron_prev", begin
      CRONS.map do |c|
        cron = ActiveJob::Cron.parse(c)
        cron ? cron.previous_time(FIXED_NOW).to_utc.to_s("%Y-%m-%dT%H:%M:%SZ") : "nil"
      end.join(",")
    end

    oracle_case "sq_recurring_task_enqueue", begin
      clean
      task = ActiveJob::SolidQueue::RecurringTask.new(
        "cache_sweeper", class_name: "A", schedule: "@daily",
        queue_name: "low", args: [1, "x"], priority: 3)
      task.enqueue_at(sq_adapter, Time.utc(2031, 1, 1, 0, 0, 0))
      run_at, task_key = sq_adapter.db.query_one(
        "SELECT run_at, task_key FROM solid_queue_jobs WHERE task_key = 'cache_sweeper'",
        as: {Time, String})
      count = sq_adapter.db.scalar(
        "SELECT count(*)::bigint FROM solid_queue_jobs WHERE task_key = 'cache_sweeper'").as(Int64)
      "#{sq_adapter.rows.first}|run_at:#{run_at.to_utc.to_s("%Y-%m-%dT%H:%M:%SZ")}" \
        "|task:#{task_key}|count:#{count}"
    end
  end
else
  it "solid_queue spec (requires PostgreSQL)" do
    pending "PostgreSQL not available"
  end
end

# Cron unit tests: fugit semantics beyond the oracle line (DOM/DOW union,
# steps/ranges/names, boundaries).
describe ActiveJob::Cron do
  it "dom/dow union when both restricted (standard cron)" do
    # 2026-10-03 is a Saturday. "0 0 1 * 1": 1st of month OR Monday.
    cron = ActiveJob::Cron.parse("0 0 1 * 1").not_nil!
    cron.next_time(Time.utc(2026, 10, 3, 12)).should eq Time.utc(2026, 10, 5)
    cron.next_time(Time.utc(2026, 10, 31, 23, 59)).should eq Time.utc(2026, 11, 1)
  end

  it "dow alone when dom is *" do
    cron = ActiveJob::Cron.parse("0 0 * * 1").not_nil!
    cron.next_time(Time.utc(2026, 10, 3)).should eq Time.utc(2026, 10, 5)
  end

  it "step ranges 9-17/2" do
    cron = ActiveJob::Cron.parse("0 9-17/2 * * *").not_nil!
    cron.next_time(Time.utc(2026, 10, 3, 12, 34, 56)).should eq Time.utc(2026, 10, 3, 13)
  end

  it "7 == sunday" do
    cron = ActiveJob::Cron.parse("0 0 * * 7").not_nil!
    cron.next_time(Time.utc(2026, 10, 3)).should eq Time.utc(2026, 10, 4)
  end

  it "month names" do
    cron = ActiveJob::Cron.parse("0 0 1 jan,jul *").not_nil!
    cron.next_time(Time.utc(2026, 10, 3)).should eq Time.utc(2027, 1, 1)
  end

  it "previous crosses year boundary" do
    cron = ActiveJob::Cron.parse("30 2 1 * *").not_nil!
    cron.previous_time(Time.utc(2026, 10, 3, 12, 34, 56)).should eq Time.utc(2026, 10, 1, 2, 30)
  end

  it "strictly-after / at-or-before boundaries" do
    cron = ActiveJob::Cron.parse("* * * * *").not_nil!
    cron.next_time(Time.utc(2026, 10, 3, 12, 34, 56)).should eq Time.utc(2026, 10, 3, 12, 35)
    cron.previous_time(Time.utc(2026, 10, 3, 12, 34, 56)).should eq Time.utc(2026, 10, 3, 12, 34)
  end
end

ActiveJob.adapter = SQ_PREV_ADAPTER if PG_OK
