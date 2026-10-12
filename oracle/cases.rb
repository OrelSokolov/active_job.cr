# Oracle cases for ActiveJob (Iteration 4).
#
# Run against the ORIGINAL gem:
#   ruby oracle/cases.rb > oracle/expected.yml
#
# Each line: "<case_id>\t<value>". The same case IDs are mirrored 1:1 in
# spec/oracle_spec.cr; the Crystal port must reproduce expected.yml
# exactly (captured from activejob 8.1.4).
#
# The :test adapter is used throughout — enqueue is recorded, not
# executed, so output is deterministic. Random fields (job_id,
# provider_job_id, enqueued_at) are stripped before printing.
gem "activejob", "= 8.1.4"
gem "activesupport", "= 8.1.4"
gem "i18n"
require "active_job"
require "date"
require "logger"

ActiveJob::Base.logger = Logger.new(File::NULL)
ActiveJob::Base.queue_adapter = :test

def render(v)
  case v
  when String then v
  else v.inspect
  end
end

def case_run(id)
  puts "#{id}\t#{render(yield)}"
rescue StandardError => e
  puts "#{id}\traise:#{e.class.name}"
end

ADAPTER = ActiveJob::Base.queue_adapter

class FlakyError < StandardError; end
class BoomError < StandardError; end

LOG = []

class LogJob < ActiveJob::Base
  def perform(a, b = nil); "done:#{a}:#{b}"; end
end

class QueueJob < ActiveJob::Base
  queue_as :low
  def perform(x); end
end

class SelfQueueJob < ActiveJob::Base
  self.queue_name = "urgent"
  def perform(x); end
end

class PrioJob < ActiveJob::Base
  queue_with_priority 5
  def perform(x); end
end

class SubQueueJob < QueueJob
  def perform(x); end
end

class CbJob < ActiveJob::Base
  before_perform { LOG << "before_perform" }
  around_perform { |j, blk| LOG << "around_in"; blk.call; LOG << "around_out" }
  after_perform  { LOG << "after_perform" }
  before_enqueue { LOG << "before_enqueue" }
  around_enqueue { |j, blk| blk.call; LOG << "around_enqueue" }
  after_enqueue  { LOG << "after_enqueue" }
  def perform(x); LOG << "perform"; "ok"; end
end

class RetryJob < ActiveJob::Base
  retry_on FlakyError, attempts: 2
  before_perform { LOG << "try" }
  def perform(x); raise FlakyError, "boom"; end
end

class RecoverJob < ActiveJob::Base
  retry_on FlakyError, attempts: 3
  def perform(x)
    raise FlakyError if executions < 2
    "recovered"
  end
end

class DiscardJob < ActiveJob::Base
  discard_on FlakyError
  def perform(x); raise FlakyError, "gone"; end
end

class PlainObject; end

# Normalize an enqueued-job hash: drop nondeterministic fields.
def norm(job)
  job = job.dup
  %w[job_id provider_job_id enqueued_at].each { |k| job.delete(k) }
  job["scheduled_at"] = "TS" if job["scheduled_at"]
  job.delete(:at)
  job
end

def last_enqueued
  norm(ADAPTER.enqueued_jobs.last).inspect
end

# --- DSL ----------------------------------------------------------
case_run("dsl_queue_default")  { LogJob.new.queue_name }
case_run("dsl_queue_as")       { QueueJob.new.queue_name }
case_run("dsl_queue_self")     { SelfQueueJob.new.queue_name }
case_run("dsl_priority")       { PrioJob.new.priority }
case_run("dsl_queue_inherited") { SubQueueJob.new.queue_name }

# --- perform_now --------------------------------------------------
case_run("now_result")       { LogJob.perform_now(1, 2) }
case_run("now_default_arg")  { LogJob.perform_now("only") }
case_run("now_not_enqueued") { ADAPTER.enqueued_jobs.size }

# --- perform_later (test adapter) ----------------------------------
case_run("later_enqueued")   { LogJob.perform_later("x", 2); last_enqueued }
case_run("later_success")    { LogJob.perform_later("x").successfully_enqueued? }
case_run("later_executions") { ADAPTER.enqueued_jobs.last["executions"] }
case_run("later_set_queue")  { LogJob.set(queue: "high").perform_later("x"); ADAPTER.enqueued_jobs.last["queue_name"] }
case_run("later_set_priority") { LogJob.set(priority: 3).perform_later("x"); ADAPTER.enqueued_jobs.last["priority"] }
case_run("later_set_wait")   { LogJob.set(wait: 10.minutes).perform_later("x"); ADAPTER.enqueued_jobs.last["scheduled_at"] ? "scheduled" : "none" }
case_run("later_set_until")  { LogJob.set(wait_until: Time.utc(2030, 1, 1, 0, 0, 0)).perform_later("x"); ADAPTER.enqueued_jobs.last["scheduled_at"] }

# --- serialization --------------------------------------------------
case_run("ser_primitives") do
  LogJob.perform_later("s", 1, 2.5, nil, true, [1, 2])
  ADAPTER.enqueued_jobs.last["arguments"]
end
case_run("ser_symbol_keys") do
  LogJob.perform_later({a: 1, "b" => 2})
  ADAPTER.enqueued_jobs.last["arguments"]
end
case_run("ser_date_time") do
  LogJob.perform_later(Date.new(2026, 1, 2), Time.utc(2026, 1, 1, 12, 0, 0))
  ADAPTER.enqueued_jobs.last["arguments"]
end
case_run("ser_unsupported") { LogJob.perform_later(PlainObject.new) }

# --- callbacks ------------------------------------------------------
case_run("cb_perform_now")   { LOG.clear; CbJob.perform_now(1); LOG }
case_run("cb_perform_later") { LOG.clear; CbJob.perform_later(1); LOG }

# --- retry / discard --------------------------------------------------
# Rails 7.1+/8.x semantics: on failure, perform_now RETURNS the caught
# exception object (does not raise); retry_on re-enqueues the job with
# executions+1; the re-raise only happens once attempts are exhausted
# on an actual (re-)execution.
case_run("retry_returns_error") { LOG.clear; RetryJob.perform_now(1).class.name }
case_run("retry_log")           { LOG }
case_run("retry_enqueued") do
  RetryJob.perform_now(1)
  last_enqueued
end
case_run("retry_recover") do
  RecoverJob.perform_now(1) # 1st execution fails, retry enqueued
  # run the enqueued retry: executions==1 -> success
  ActiveJob::Base.deserialize(ADAPTER.enqueued_jobs.last).perform_now
end
case_run("retry_exhausted") do
  RetryJob.perform_now(1) # 1st execution fails, retry enqueued (executions 1)
  # 2nd execution reaches attempts: 2 -> re-raise
  ActiveJob::Base.deserialize(ADAPTER.enqueued_jobs.last).perform_now
end
case_run("discard_returns_error") { DiscardJob.perform_now(1).class.name }

# --- deserialize roundtrip ---------------------------------------------
case_run("deserialize_roundtrip") do
  ActiveJob::Base.deserialize(LogJob.new("x", 9).serialize).perform_now
end
