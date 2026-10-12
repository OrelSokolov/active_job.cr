# Smoke spec for the async adapter (in-process fiber pool): the
# oracle suite covers semantics via the test adapter; this covers
# concurrent execution + drain behavior.

require "spec"
require "../src/active_job_cr"

class CountJob < ActiveJob::Base
  class_property count = 0

  def perform(x)
    CountJob.count += x.as(Int32)
  end
end

describe "async adapter" do
  it "performs every enqueued job exactly once" do
    adapter = ActiveJob::AsyncAdapter.new(pool_size: 4)
    ActiveJob.adapter = adapter
    CountJob.count = 0

    200.times { |i| CountJob.perform_later(i) }
    adapter.wait_empty

    CountJob.count.should eq((0...200).sum)
    adapter.performed.should eq(200)
    ActiveJob.adapter = ActiveJob::TestAdapter.new
  end
end
