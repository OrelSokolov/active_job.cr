# RedisQueueAdapter spec (Iteration 7): queue contract subset —
# enqueue/claim FIFO, scheduled gating, recurring dedup, perform
# roundtrip. Requires redis-server; skips when absent.

require "spec"
require "../src/active_job_cr"
require "../src/adapters/redis_queue"

RQ_OK = begin
  `redis-cli -u #{ENV.fetch("REDIS_URL_TEST", "redis://127.0.0.1:6379/14")} ping 2>/dev/null` \
    .strip == "PONG"
rescue ex
  false
end

RQ_ADAPTER = RQ_OK ? ActiveJob::RedisQueueAdapter.new(
  url: ENV.fetch("REDIS_URL_TEST", "redis://127.0.0.1:6379/14")) : nil

def rq_adapter
  RQ_ADAPTER.not_nil!
end

RQ_LOG = [] of String

class RqA < ActiveJob::Base
  def perform(x = nil)
    RQ_LOG << "A:#{x}"
    nil
  end
end

class RqLow < ActiveJob::Base
  queue_as :low

  def perform(x = nil)
    RQ_LOG << "L:#{x}"
    nil
  end
end

RQ_PREV_ADAPTER = ActiveJob.adapter

if RQ_OK
  describe ActiveJob::RedisQueueAdapter do
    before_each do
      ActiveJob.adapter = rq_adapter
      rq_adapter.clear
      RQ_LOG.clear
    end

    after_each do
      ActiveJob.adapter = RQ_PREV_ADAPTER
    end

    it "enqueue → claim FIFO" do
      RqA.perform_later(1)
      RqA.perform_later(2)
      RqA.perform_later(3)
      claimed = rq_adapter.claim("default", 10)
      claimed.map(&.["job_class"].as(String)).should eq ["RqA", "RqA", "RqA"]
      args = claimed.map { |d| d["arguments"].as(Array(ActiveJob::ArgValue))[0] }
      args.should eq [1, 2, 3]
    end

    it "claim respects queue list and limit" do
      RqLow.perform_later(1)
      RqA.perform_later(1)
      claimed = rq_adapter.claim(["low", "default"], 1)
      claimed.size.should eq 1
      claimed.first["queue_name"].should eq "low"
      rest = rq_adapter.claim(["low", "default"], 10)
      rest.map(&.["queue_name"].as(String)).should eq ["default"]
    end

    it "scheduled not claimable until due" do
      RqA.set(wait_until: Time.utc + 1.hour).perform_later
      rq_adapter.claim("default", 10).size.should eq 0
      rq_adapter.scheduled_count.should eq 1
    end

    it "scheduled promoted when due" do
      RqA.set(wait_until: Time.utc - 1.second).perform_later
      claimed = rq_adapter.claim("default", 10)
      claimed.size.should eq 1
      rq_adapter.scheduled_count.should eq 0
    end

    it "perform roundtrip" do
      RqA.perform_later(9)
      rq_adapter.perform_one(rq_adapter.claim("default", 1).first)
      RQ_LOG.should eq ["A:9"]
      rq_adapter.pending_count.should eq 0
    end

    it "recurring enqueue dedups on task_key+run_at" do
      record = ActiveJob::EnqueuedJob.new("RqA", "default", nil,
        [] of ActiveJob::ArgValue, 0, {} of String => Int32,
        Time.utc, RqA)
      at = Time.utc(2031, 1, 1)
      2.times { rq_adapter.enqueue_recurring("sweep", at, record) }
      rq_adapter.claim("default", 10).size.should eq 1
    end
  end
else
  it "redis queue adapter (requires redis-server)" do
    pending "redis-server not available"
  end
end
