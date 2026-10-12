# Async adapter: in-process fiber pool (Rails' :async semantic — jobs
# run immediately inside the owning process, no broker). Fibers are
# cooperative on Crystal's event loop, so counters need no locks.

module ActiveJob
  class AsyncAdapter
    include Adapter

    def initialize(@pool_size : Int32 = 8, @queue_size : Int32 = 1024)
      @channel = Channel(EnqueuedJob).new(@queue_size)
      @started = false
      @in_flight = 0
      @done = Channel(Nil).new
    end

    getter performed = 0

    def enqueue(record : EnqueuedJob) : Nil
      start unless @started
      @in_flight += 1
      @channel.send(record)
    end

    # Block until every enqueued job has been performed (bench seam).
    # Fibers are cooperative: yielding lets workers drain the queue.
    def wait_empty : Nil
      while @in_flight > 0
        Fiber.yield
      end
    end

    def shutdown : Nil
      @channel.close if @started
      @started = false
    end

    private def start : Nil
      @pool_size.times do
        spawn worker_loop
      end
      @started = true
    end

    private def worker_loop : Nil
      loop do
        record = @channel.receive?
        break unless record
        begin
          Base.deserialize(record.to_job_data).perform_now
          @performed += 1
        rescue ex
          # Rails' async adapter lets the worker's rescue handling deal
          # with failures; retry/discard logic already ran in perform_now.
        ensure
          @in_flight -= 1
        end
      end
    end
  end
end
