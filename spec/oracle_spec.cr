# Oracle spec: the Crystal port must reproduce oracle/expected.yml
# exactly (captured from activejob 8.1.4 via oracle/cases.rb).
# Same case IDs, same expectations, no edits on either side.

require "spec"
require "../src/active_job_cr"

EXPECTED = begin
  hash = {} of String => String
  current_id : String? = nil
  File.read("#{__DIR__}/../oracle/expected.yml").each_line do |line|
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

class FlakyError < Exception; end

LOG = [] of String

class LogJob < ActiveJob::Base
  def perform(a, b = nil)
    "done:#{a}:#{b}"
  end
end

class QueueJob < ActiveJob::Base
  queue_as :low

  def perform(x)
  end
end

class SelfQueueJob < ActiveJob::Base
  queue_as "urgent"

  def perform(x)
  end
end

class PrioJob < ActiveJob::Base
  queue_with_priority 5

  def perform(x)
  end
end

class SubQueueJob < QueueJob
  def perform(x)
  end
end

class CbJob < ActiveJob::Base
  before_perform { LOG << "before_perform" }
  around_perform { |job, blk| LOG << "around_in"; blk.call; LOG << "around_out" }
  after_perform { LOG << "after_perform" }
  before_enqueue { LOG << "before_enqueue" }
  around_enqueue { |job, blk| blk.call; LOG << "around_enqueue" }
  after_enqueue { LOG << "after_enqueue" }

  def perform(x)
    LOG << "perform"
    "ok"
  end
end

class RetryJob < ActiveJob::Base
  retry_on FlakyError, attempts: 2
  before_perform { LOG << "try" }

  def perform(x)
    raise FlakyError.new("boom")
  end
end

class RecoverJob < ActiveJob::Base
  retry_on FlakyError, attempts: 3

  def perform(x)
    raise FlakyError.new if executions < 2
    "recovered"
  end
end

class DiscardJob < ActiveJob::Base
  discard_on FlakyError

  def perform(x)
    raise FlakyError.new("gone")
  end
end

class PlainObject; end

ADAPTER = ActiveJob::TestAdapter.new
ActiveJob.adapter = ADAPTER

# Normalize an enqueued record's Ruby-style inspect: mask the random
# retry timestamp and strip the `at:` epoch (both nondeterministic).
def norm(rec : ActiveJob::EnqueuedJob) : String
  s = rec.ruby_inspect
  s = s.gsub(/"scheduled_at" => "[^"]*"/, %q{"scheduled_at" => "TS"})
  s = s.gsub(/, at: [0-9.]+/, "")
  s
end

def last_enqueued : String
  norm(ADAPTER.enqueued_jobs.last)
end

CASES = {
  # --- DSL ---
  "dsl_queue_default"   => -> { LogJob.new.queue_name },
  "dsl_queue_as"        => -> { QueueJob.new.queue_name },
  "dsl_queue_self"      => -> { SelfQueueJob.new.queue_name },
  "dsl_priority"        => -> { PrioJob.new.priority.to_s },
  "dsl_queue_inherited" => -> { SubQueueJob.new.queue_name },
  # --- perform_now ---
  "now_result"       => -> { LogJob.perform_now(1, 2).to_s },
  "now_default_arg"  => -> { LogJob.perform_now("only").to_s },
  "now_not_enqueued" => -> { ADAPTER.enqueued_jobs.size.to_s },
  # --- perform_later ---
  "later_enqueued"      => -> { LogJob.perform_later("x", 2); last_enqueued },
  "later_success"       => -> { LogJob.perform_later("x").successfully_enqueued?.to_s },
  "later_executions"    => -> { ADAPTER.enqueued_jobs.last.executions.to_s },
  "later_set_queue"     => -> { LogJob.set(queue: "high").perform_later("x"); ADAPTER.enqueued_jobs.last.queue_name },
  "later_set_priority"  => -> { LogJob.set(priority: 3).perform_later("x"); ADAPTER.enqueued_jobs.last.priority.to_s },
  "later_set_wait"      => -> { LogJob.set(wait: 10.minutes).perform_later("x"); ADAPTER.enqueued_jobs.last.scheduled_at ? "scheduled" : "none" },
  "later_set_until"     => -> { LogJob.set(wait_until: Time.utc(2030, 1, 1, 0, 0, 0)).perform_later("x"); ActiveJob::Serialization.iso8601_ns(ADAPTER.enqueued_jobs.last.scheduled_at.not_nil!) },
  # --- serialization ---
  "ser_primitives"   => -> { LogJob.perform_later("s", 1, 2.5, nil, true, [1, 2]); ADAPTER.enqueued_jobs.last.arguments.inspect },
  "ser_symbol_keys"  => -> { LogJob.perform_later({:a => 1, "b" => 2} of String | Symbol => Int32); ADAPTER.enqueued_jobs.last.arguments.inspect },
  "ser_date_time"    => -> { LogJob.perform_later(Date.new(2026, 1, 2), Time.utc(2026, 1, 1, 12, 0, 0)); ADAPTER.enqueued_jobs.last.arguments.inspect },
  "ser_unsupported"  => -> { LogJob.perform_later(PlainObject.new).to_s },
  # --- callbacks ---
  "cb_perform_now"   => -> { LOG.clear; CbJob.perform_now(1); LOG.inspect },
  "cb_perform_later" => -> { LOG.clear; CbJob.perform_later(1); LOG.inspect },
  # --- retry / discard ---
  "retry_returns_error" => -> { LOG.clear; RetryJob.perform_now(1).class.name },
  "retry_log"           => -> { LOG.inspect },
  "retry_enqueued"      => -> { RetryJob.perform_now(1); last_enqueued },
  "retry_recover"       => -> { RecoverJob.perform_now(1); ActiveJob::Base.deserialize(ADAPTER.enqueued_jobs.last.to_job_data).perform_now.to_s },
  "retry_exhausted"     => -> { RetryJob.perform_now(1); ActiveJob::Base.deserialize(ADAPTER.enqueued_jobs.last.to_job_data).perform_now.to_s },
  "discard_returns_error" => -> { DiscardJob.perform_now(1).class.name },
  # --- deserialize roundtrip ---
  "deserialize_roundtrip" => -> { ActiveJob::Base.deserialize(LogJob.new("x", 9).serialize).perform_now.to_s },
}

describe "active_job oracle (activejob 8.1.4)" do
  EXPECTED.each do |id, expected|
    it id do
      block = CASES[id]?
      block.should_not be_nil, "case #{id} not mirrored in spec"
      result = begin
        block.not_nil!.call.to_s
      rescue ex : Exception
        "raise:#{ex.class.name}"
      end
      result.strip.should eq(expected.strip), <<-MSG
        #{id}:
          expected: #{expected.inspect}
          got:      #{result.inspect}
        MSG
    end
  end

  it "mirrors every oracle case" do
    missing = EXPECTED.keys - CASES.keys
    extra = CASES.keys - EXPECTED.keys
    missing.should be_empty
    extra.should be_empty
  end
end
