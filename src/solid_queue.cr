# ActiveJob::SolidQueue — durable PG queue, the port's analog of Rails 8's
# solid_queue default (oracle: oracle/solid_queue_cases.rb captured from
# solid_queue 1.2.4; the spec mirrors every case 1:1).
#
# Observable semantics mirrored (spec/solid_queue_spec.cr):
# - claim order: priority ASC, FIFO by id, FOR UPDATE SKIP LOCKED
# - scheduled jobs: invisible to claim until scheduled_at <= now
# - enqueue rows: queue_name / class / serialized ActiveJob payload /
#   priority (nil → 0)
# - retry: ActiveJob-level (retry_on) re-enqueues a NEW row; the failed
#   attempt's row is marked finished when the error was handled
# - recurring: cron-scheduled (fugit semantics via ActiveJob::Cron) tasks
#   with (task_key, run_at) uniqueness
#
# Deviations from solid_queue (documented): no multi-table
# ready/claimed/scheduled execution split (a single jobs table with
# claimed_at/finished_at columns); no concurrency controls, pauses or
# process heartbeats (P2); claimed jobs of a crashed worker are
# reclaimed after `stale_claim_seconds` (default 1 hour).

require "pg"
require "json"
require "./active_job_cr"
require "./cron"

module ActiveJob
  module SolidQueue
    class Adapter
      include ActiveJob::Adapter

      getter db : DB::Database
      getter stale_claim_seconds : Int32

      def initialize(@db : DB::Database, @stale_claim_seconds : Int32 = 3600)
        ensure_schema
      end

      def self.connect(url : String = ENV.fetch("DATABASE_URL", "postgres:///"),
                       stale_claim_seconds : Int32 = 3600) : Adapter
        new(DB.open(url), stale_claim_seconds)
      end

      # --- QueueAdapter port -------------------------------------------------
      # enqueue + enqueue_at ride the same call: scheduled_at is on the
      # record (same seam as the test adapter).

      def enqueue(record : EnqueuedJob) : Nil
        sql = <<-SQL
          INSERT INTO solid_queue_jobs
            (queue_name, class_name, arguments, priority, scheduled_at)
          VALUES ($1, $2, $3, $4, COALESCE($5::timestamptz, now()))
        SQL
        @db.exec(sql, record.queue_name, record.job_class,
                 serialize_payload(record), priority_value(record),
                 record.scheduled_at.try(&.to_utc))
      end

      # --- queue operations ----------------------------------------------------

      # Claims up to `limit` due jobs from `queues` (priority DESC — higher
      # number runs first, verified against solid_queue 1.2.4 — then FIFO by
      # id, FOR UPDATE SKIP LOCKED). Returns job data hashes ready for
      # Base.deserialize, carrying provider_job_id (the row id).
      def claim(queues : Array(String), limit : Int32) : Array(Hash(String, ArgValue))
        return [] of Hash(String, ArgValue) if limit <= 0 || queues.empty?
        claimed = [] of Hash(String, ArgValue)

        @db.using_connection do |conn|
          conn.transaction do |tx|
            c = tx.connection
            rows = [] of ClaimedRow
            c.query_each(<<-SQL, args: [queues]) do |rs|
              SELECT id, class_name, arguments, queue_name, priority, scheduled_at
              FROM solid_queue_jobs
              WHERE queue_name = ANY($1)
                AND finished_at IS NULL
                AND claimed_at IS NULL
                AND scheduled_at <= now()
              ORDER BY priority DESC, id ASC
              LIMIT #{limit}
              FOR UPDATE SKIP LOCKED
            SQL
              rows << ClaimedRow.new(rs.read(Int64), rs.read(String), rs.read(String),
                rs.read(String), rs.read(Int32), rs.read(Time?))
            end
            unless rows.empty?
              ids = rows.map(&.id)
              c.exec("UPDATE solid_queue_jobs SET claimed_at = now() WHERE id = ANY($1)", ids)
            end
            rows.each { |r| claimed << job_data(r) }
          end
        end
        claimed
      end

      def claim(queue : String, limit : Int32) : Array(Hash(String, ArgValue))
        claim([queue], limit)
      end

      # Marks a performed job finished (the analog of
      # ClaimedExecution#finished — the row stays for history).
      def finished(data : Hash(String, ArgValue)) : Nil
        @db.exec("UPDATE solid_queue_jobs SET finished_at = now() WHERE id = $1",
                 data["provider_job_id"].as(Int64))
      end

      # Releases a claimed job without finishing it (perform_now raised an
      # unhandled error): the row becomes claimable again.
      def release(data : Hash(String, ArgValue)) : Nil
        @db.exec("UPDATE solid_queue_jobs SET claimed_at = NULL WHERE id = $1",
                 data["provider_job_id"].as(Int64))
      end

      # Enqueue for recurring tasks: records task_key + run_at, dedups on
      # (task_key, run_at) — the analog of RecurringExecution + unique
      # index. The job itself is ready immediately (scheduled_at = now()):
      # like solid_queue, the dispatcher enqueues AT the occurrence time,
      # run_at records which occurrence this is.
      def enqueue_recurring(task_key : String, run_at : Time,
                            record : EnqueuedJob) : Nil
        sql = <<-SQL
          INSERT INTO solid_queue_jobs
            (queue_name, class_name, arguments, priority, scheduled_at,
             task_key, run_at)
          VALUES ($1, $2, $3, $4, now(), $6, $5)
          ON CONFLICT (task_key, run_at) WHERE task_key IS NOT NULL DO NOTHING
        SQL
        @db.exec(sql, record.queue_name, record.job_class,
                 serialize_payload(record), priority_value(record),
                 run_at.to_utc, task_key)
      end

      # Reclaims stale claims of dead workers (crash recovery).
      def release_stale_claims : Nil
        @db.exec(<<-SQL, stale_claim_seconds)
          UPDATE solid_queue_jobs SET claimed_at = NULL
          WHERE claimed_at IS NOT NULL
            AND claimed_at < now() - make_interval(secs => $1)
        SQL
      end

      # --- worker ---------------------------------------------------------------

      # Runs the worker loop in the current fiber until `stop` is closed.
      def work(queues : Array(String) = ["default"], batch : Int32 = 10,
               poll_interval : Time::Span = 0.1.seconds,
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
          finished(data)
        rescue ex : Exception
          # perform_now only raises UNhandled errors (retry_on/discard_on
          # swallow their errors) — releasing makes the row claimable again.
          release(data)
        end
      end


      # Scheduler tick: enqueue every task's next occurrence (dedup makes
      # it idempotent). Call periodically from a dispatcher fiber.
      def schedule_recurring(tasks : Array(RecurringTask),
                             now : Time = Time.utc) : Nil
        tasks.each do |task|
          task.enqueue_at(self, task.cron.next_time(now))
        end
      end

      # --- introspection (spec/bench seam) -----------------------------------------

      def pending_count(queue : String? = nil) : Int32
        if queue
          @db.scalar(<<-SQL, queue).as(Int64).to_i
            SELECT count(*)::bigint FROM solid_queue_jobs
            WHERE finished_at IS NULL AND queue_name = $1
          SQL
        else
          @db.scalar(<<-SQL).as(Int64).to_i
            SELECT count(*)::bigint FROM solid_queue_jobs WHERE finished_at IS NULL
          SQL
        end
      end

      def scheduled_count : Int32
        @db.scalar(<<-SQL).as(Int64).to_i
          SELECT count(*)::bigint FROM solid_queue_jobs
          WHERE finished_at IS NULL AND scheduled_at > now()
        SQL
      end

      # First n scheduled (unfinished, future) run times, earliest first —
      # the scheduler ordering (scheduled_at ASC, priority ASC, id ASC).
      def scheduled_first(n : Int32) : Array(Time)
        times = [] of Time
        @db.query_each(<<-SQL) do |rs|
          SELECT scheduled_at FROM solid_queue_jobs
          WHERE finished_at IS NULL AND claimed_at IS NULL AND scheduled_at > now()
          ORDER BY scheduled_at ASC, priority DESC, id ASC
          LIMIT #{n}
        SQL
          times << rs.read(Time)
        end
        times
      end

      def claimed_count : Int32
        @db.scalar(<<-SQL).as(Int64).to_i
          SELECT count(*)::bigint FROM solid_queue_jobs
          WHERE finished_at IS NULL AND claimed_at IS NOT NULL
        SQL
      end

      def last_run_at(task_key : String) : Time?
        @db.scalar(<<-SQL, task_key).as?(Time)
          SELECT max(run_at) FROM solid_queue_jobs WHERE task_key = $1
        SQL
      end

      # Raw row rendering in the oracle's row() format (spec mirror).
      def rows : Array(String)
        list = [] of String
        @db.query_each(<<-SQL) do |rs|
          SELECT queue_name, class_name, priority, arguments, scheduled_at, finished_at
          FROM solid_queue_jobs ORDER BY id ASC
        SQL
          list << render_row(rs)
        end
        list
      end

      def clear : Nil
        @db.exec("DELETE FROM solid_queue_jobs")
      end

      # --- schema --------------------------------------------------------------------

      def ensure_schema : Nil
        @db.exec(<<-SQL)
          CREATE TABLE IF NOT EXISTS solid_queue_jobs (
            id BIGSERIAL PRIMARY KEY,
            queue_name TEXT NOT NULL,
            class_name TEXT NOT NULL,
            arguments TEXT NOT NULL,
            priority INT NOT NULL DEFAULT 0,
            scheduled_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            claimed_at TIMESTAMPTZ,
            finished_at TIMESTAMPTZ,
            task_key TEXT,
            run_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
          )
        SQL
        @db.exec(<<-SQL)
          CREATE UNIQUE INDEX IF NOT EXISTS solid_queue_jobs_recurring_idx
          ON solid_queue_jobs (task_key, run_at)
          WHERE task_key IS NOT NULL
        SQL
        @db.exec(<<-SQL)
          CREATE INDEX IF NOT EXISTS solid_queue_jobs_claim_idx
          ON solid_queue_jobs (queue_name, finished_at, claimed_at, scheduled_at)
        SQL
      end

      # --- job (de)serialization ---------------------------------------------------

      private record ClaimedRow, id : Int64, class_name : String, arguments : String,
                       queue_name : String, priority : Int32, scheduled_at : Time?

      # Stored payload = the ActiveJob serialize hash (job_class ..
      # timezone) minus volatile identity (job_id/enqueued_at/scheduled_at)
      # — the same payload solid_queue keeps in its arguments column.
      def serialize_payload(record : EnqueuedJob) : String
        data = record.to_job_data
        JSON.build do |io|
          io.object do
            data.each do |k, v|
              next if {"job_id", "enqueued_at", "scheduled_at"}.includes?(k)
              io.field(k) { arg_write(io, v) }
            end
            io.field("locale") { io.string("en") }
            io.field("timezone") { io.null }
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

      private def job_data(row : ClaimedRow) : Hash(String, ArgValue)
        payload = JSON.parse(row.arguments)
        data = {
          "job_class"       => row.class_name.as(ArgValue),
          "queue_name"      => row.queue_name.as(ArgValue),
          "priority"        => row.priority.as(ArgValue),
          "provider_job_id" => row.id.as(ArgValue),
          "executions"      => 0,
        } of String => ArgValue
        if exec = payload["executions"]?
          data["executions"] = json_to_arg_value(exec)
        end
        if sched = row.scheduled_at
          data["scheduled_at"] = Serialization.iso8601_ns(sched.to_utc)
        end
        args = payload["arguments"]?
        data["arguments"] = args ? json_to_arg_value(args) : [] of ArgValue
        data
      end

      def json_to_arg_value(v : JSON::Any) : ArgValue
        case raw = v.raw
        when Nil, Bool, String then raw.as(ArgValue)
        when Int64
          i = raw.as(Int64)
          (i >= Int32::MIN && i <= Int32::MAX) ? Int32.new(i) : i.as(ArgValue)
        when Float64 then raw.as(Float64).as(ArgValue)
        when Array
          arr = [] of ArgValue
          raw.as(Array(JSON::Any)).each { |x| arr << json_to_arg_value(x) }
          arr.as(ArgValue)
        when Hash
          h = {} of String | Symbol => ArgValue
          raw.as(Hash(String, JSON::Any)).each do |k, x|
            h[k] = json_to_arg_value(x)
          end
          h.as(ArgValue)
        else
          raise SerializationError.new("unsupported JSON value #{v.raw.class}")
        end
      end

      private def priority_value(record : EnqueuedJob) : Int32
        record.priority || 0
      end

      # --- oracle-format rendering (spec mirror) ---------------------------------

      private def render_row(rs : DB::ResultSet) : String
        queue_name = rs.read(String)
        class_name = rs.read(String)
        priority = rs.read(Int32)
        arguments = rs.read(String)
        scheduled_at = rs.read(Time?)
        finished_at = rs.read(Time?)

        hash = JSON.parse(arguments).as_h
        sched_str = if t = scheduled_at
          t.to_utc > Time.utc + 1.hour ? t.to_utc.to_s("%Y-%m-%dT%H:%M:%SZ") : "now"
        else
          "nil"
        end
        "#{queue_name}|#{class_name}|#{priority}|" \
          "#{render_args(hash)}|sched:#{sched_str}|fin:#{!finished_at.nil?}"
      end

      # Renders the stored payload the way the oracle's row() does after
      # stripping volatile keys: sorted ruby-inspected key=value pairs.
      private def render_args(hash : Hash(String, JSON::Any)) : String
        skipped = {"job_id", "enqueued_at", "scheduled_at", "provider_job_id"}
        hash.reject(skipped).keys.sort.map do |k|
          "#{k}=#{ruby_inspect(hash[k])}"
        end.join(" ")
      end

      private def ruby_inspect(v : JSON::Any) : String
        case v.raw
        when Nil then "nil"
        when Bool then v.raw.to_s
        when Int64 then v.raw.to_s
        when Float64 then v.raw.to_s
        when String then v.raw.inspect
        when Array then "[" + v.raw.as(Array(JSON::Any)).map { |x| ruby_inspect(x) }.join(", ") + "]"
        when Hash then "{}"
        else v.raw.inspect
        end
      end
    end

    # --- recurring --------------------------------------------------------------

    # A cron-scheduled task (solid_queue RecurringTask):
    #   RecurringTask.new("cache-sweeper", class_name: "SweepJob",
    #                     schedule: "@daily", queue: "low",
    #                     priority: 3, args: [1, "x"])
    class RecurringTask
      getter key : String
      getter class_name : String
      getter cron : Cron
      getter queue_name : String
      getter priority : Int32?
      getter arguments : Array(ArgValue)

      def initialize(@key : String, @class_name : String, schedule : String,
                     @queue_name : String = "default",
                     @priority : Int32? = nil, args : Array = [] of Nil)
        parsed = Cron.parse(schedule)
        unless parsed
          raise Cron::ParseError.new(
            "invalid schedule #{schedule.inspect} for recurring task #{key}")
        end
        @cron = parsed
        converted = [] of ArgValue
        args.each do |a|
          converted << Serialization.serialize_arg(Serialization.to_arg(a)).as(ArgValue)
        end
        @arguments = converted
      end

      # Builds the enqueue record for the occurrence at `at`. The job
      # class is resolved from the compile-time registry — runtime
      # instantiation by name is not a thing in Crystal.
      def build_record(at : Time) : EnqueuedJob
        job_class = Base.find_job_class(@class_name)
        unless job_class
          raise ArgumentError.new(
            "Unknown job class #{@class_name} for recurring task #{key}")
        end
        EnqueuedJob.new(@class_name, @queue_name, @priority, @arguments,
                        0, {} of String => Int32, at, job_class)
      end

      def enqueue_at(adapter : Adapter, at : Time) : Nil
        adapter.enqueue_recurring(@key, at, build_record(at))
      end
    end  end
end
