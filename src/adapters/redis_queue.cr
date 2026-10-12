# ActiveJob::RedisQueueAdapter — Redis queue behind the QueueAdapter
# port, for the monolith's "Redis — штатная опция" (PLAN Iteration 7).
#
# Shape: one LIST per queue (LPUSH + RPOP → FIFO), one ZSET for
# scheduled jobs (score = epoch seconds, promoted by atomic ZPOPMIN via
# eval), recurring dedup via SET NX markers. Ephemeral: no finished-job
# history, no priority ordering inside a queue, no stale-claim reclaim
# (jobs are popped by exactly one worker) — all documented deviations
# from the PG solid_queue adapter, which is the durable default.

require "redis"
require "json"

module ActiveJob
  class RedisQueueAdapter
    include ActiveJob::Adapter

    def initialize(url : String = ENV.fetch("REDIS_URL", "redis:///"))
      @redis = ::Redis::Client.new(URI.parse(url))
    end

    def enqueue(record : EnqueuedJob) : Nil
      payload = serialize_payload(record)
      if scheduled = record.scheduled_at
        @redis.zadd("sq:scheduled", scheduled.to_utc.to_unix_f, payload)
      else
        @redis.lpush("sq:queue:#{record.queue_name}", payload)
      end
    end

    # Claims up to `limit` due jobs from `queues`, promoting due
    # scheduled jobs first (FIFO by score). Returns job data hashes for
    # Base.deserialize.
    def claim(queues : Array(String), limit : Int32) : Array(Hash(String, ArgValue))
      return [] of Hash(String, ArgValue) if limit <= 0
      promote_due_scheduled(limit)
      claimed = [] of Hash(String, ArgValue)
      queues.each do |queue|
        while claimed.size < limit
          payload = @redis.rpop("sq:queue:#{queue}")
          break unless payload
          claimed << parse_payload(payload.to_s)
        end
      end
      claimed
    end

    def claim(queue : String, limit : Int32) : Array(Hash(String, ArgValue))
      claim([queue], limit)
    end

    # No history: performing pops for good. These exist so worker code
    # is adapter-agnostic.
    def finished(data : Hash(String, ArgValue)) : Nil
    end

    def release(data : Hash(String, ArgValue)) : Nil
      @redis.lpush("sq:queue:dead", serialize_data(data))
    end

    def enqueue_recurring(task_key : String, run_at : Time,
                          record : EnqueuedJob) : Nil
      marker = "sq:recurring:#{task_key}:#{run_at.to_utc.to_unix}"
      return unless @redis.set(marker, "1", nx: true)
      enqueue(record)
    end

    def work(queues : Array(String) = ["default"], batch : Int32 = 10,
             poll_interval : Time::Span = 0.01.seconds,
             stop : Channel(Nil)? = nil) : Nil
      until stop.try(&.closed?)
        jobs = claim(queues, batch)
        if jobs.empty?
          break if stop.try(&.closed?)
          sleep poll_interval.total_seconds
        else
          jobs.each { |data| perform_one(data) }
        end
      end
    end

    def perform_one(data : Hash(String, ArgValue)) : Nil
      job = Base.deserialize(data)
      begin
        job.perform_now
      rescue ex : Exception
        release(data)
      end
    end

    def pending_count : Int64
      keys = @redis.keys("sq:queue:*")
      return 0_i64 if keys.empty?
      keys.map { |k| llen(k.to_s).to_i64 }.sum
    end

    def scheduled_count : Int64
      @redis.zcard("sq:scheduled")
    end

    def clear : Nil
      keys = @redis.keys("sq:*")
      str_keys = keys.map(&.to_s)
      @redis.del(str_keys) unless str_keys.empty?
    end

    # --- internals -------------------------------------------------------

    # Move due scheduled jobs onto their queue lists, atomically per job
    # (ZRANGEBYSCORE to peek, ZREM to win the job — a job claimed by the
    # ZREM is LPUSHed exactly once).
    private def promote_due_scheduled(limit : Int32) : Nil
      now = Time.utc.to_unix_f
      due = @redis.zrangebyscore("sq:scheduled", "-inf", now,
        limit: ["0", limit.to_s])
      return if due.empty?
      due.each do |payload|
        next unless @redis.zrem("sq:scheduled", payload.to_s) > 0
        data = parse_payload(payload.to_s)
        @redis.lpush("sq:queue:#{data["queue_name"].as(String)}", payload.to_s)
      end
    end

    private def serialize_payload(record : EnqueuedJob) : String
      data = record.to_job_data
      data["arguments"] = record.arguments
      JSON.build do |io|
        io.object do
          data.each do |k, v|
            next if k == "job_id" || k == "enqueued_at"
            io.field(k) { arg_write(io, v) }
          end
        end
      end
    end

    private def serialize_data(data : Hash(String, ArgValue)) : String
      JSON.build do |io|
        io.object do
          data.each do |k, v|
            io.field(k) { arg_write(io, v) }
          end
        end
      end
    end

    private def arg_write(io : JSON::Builder, v : ArgValue) : Nil
      case v
      when Nil then io.null
      when Bool then io.bool(v)
      when Int32, Int64 then io.number(v.as(Int::Primitive))
      when Float64 then io.number(v)
      when String then io.string(v)
      when Symbol then io.string(v.to_s)
      when Date then io.string(v.to_s)
      when Time then io.string(Serialization.iso8601_ns(v))
      when Array
        io.array do
          v.each { |x| arg_write(io, x.as(ArgValue)) }
        end
      when Hash
        io.object do
          v.each do |k, x|
            io.field(k.to_s) { arg_write(io, x) }
          end
        end
      else
        raise SerializationError.new("unsupported argument type #{v.class}")
      end
    end

    private def parse_payload(payload : String) : Hash(String, ArgValue)
      any = JSON.parse(payload)
      queue_name = any["queue_name"]? || JSON::Any.new("default")
      data = {
        "job_class"  => any["job_class"].as_s.as(ArgValue),
        "queue_name" => queue_name.as_s.as(ArgValue),
        "executions" => 0_i32,
      } of String => ArgValue
      if exec = any["executions"]?
        data["executions"] = json_to_arg(exec)
      end
      if sched = any["scheduled_at"]?
        data["scheduled_at"] = json_to_arg(sched)
      end
      args = any["arguments"]?
      data["arguments"] = args ? json_to_arg(args) : [] of ArgValue
      data
    end

    private def json_to_arg(v : JSON::Any) : ArgValue
      case raw = v.raw
      when Nil, Bool, String then raw.as(ArgValue)
      when Int64
        i = raw.as(Int64)
        i >= Int32::MIN && i <= Int32::MAX ? Int32.new(i) : i
      when Float64 then raw
      when Array
        arr = [] of ArgValue
        raw.as(Array(JSON::Any)).each { |x| arr << json_to_arg(x) }
        arr
      when Hash
        h = {} of String | Symbol => ArgValue
        raw.as(Hash(String, JSON::Any)).each { |k, x| h[k] = json_to_arg(x) }
        h
      else
        raise SerializationError.new("unsupported JSON value")
      end
    end

    # Fallbacks for client methods the shard types differently.
    private def llen(key : String)
      @redis.llen(key)
    end
  end
end
