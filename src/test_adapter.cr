# Test adapter: records enqueues without executing (Rails' :test).

module ActiveJob
  # One enqueued job, mirroring the fields of Rails' test-adapter
  # records. `ruby_inspect` renders the exact format Ruby's Hash#inspect
  # produces (string keys first, then symbol extras) — the oracle
  # compares this string byte-for-byte.
  class EnqueuedJob
    getter job_class : String
    getter queue_name : String
    getter priority : Int32?
    getter arguments : Array(ArgValue)
    getter executions : Int32
    getter exception_executions : Hash(String, Int32)
    getter scheduled_at : Time?
    getter job : Base.class
    getter at : Time?

    def initialize(@job_class, @queue_name, @priority, @arguments,
                   @executions, @exception_executions, @scheduled_at, @job)
      # The moment the adapter was asked to run the job (retries set
      # this; direct enqueues leave it nil, like Ruby's `at`).
      @at = @scheduled_at
    end

    def ruby_inspect : String
      String.build do |io|
        io << "{\"job_class\" => " << @job_class.inspect
        io << ", \"queue_name\" => " << @queue_name.inspect
        io << ", \"priority\" => " << (@priority.nil? ? "nil" : @priority.to_s)
        io << ", \"arguments\" => " << inspect_args
        io << ", \"executions\" => " << @executions
        io << ", \"exception_executions\" => " << @exception_executions.inspect
        io << ", \"locale\" => \"en\""
        io << ", \"timezone\" => nil"
        io << ", \"scheduled_at\" => " << (@scheduled_at ? @scheduled_at.to_s.inspect : "nil")
        io << ", job: " << @job_class
        io << ", args: " << inspect_args
        io << ", queue: " << @queue_name.inspect
        io << ", priority: " << (@priority.nil? ? "nil" : @priority.to_s)
        io << ", at: " << @at.not_nil!.to_unix_f if @at
        io << "}"
      end
    end

    # Hash form for ActiveJob::Base.deserialize.
    def to_job_data : Hash(String, ArgValue)
      ee = {} of String | Symbol => ArgValue
      @exception_executions.each { |k, v| ee[k] = v }
      {
        "job_class"            => @job_class,
        "queue_name"           => @queue_name,
        "priority"             => @priority,
        "arguments"            => @arguments,
        "executions"           => @executions,
        "exception_executions" => ee,
        "scheduled_at"         => @scheduled_at.try { |t| Serialization.iso8601_ns(t) },
      } of String => ArgValue
    end

    # Ruby Array#inspect rendering (identical to Crystal's for these
    # value kinds, except Hash keys order is preserved).
    private def inspect_args : String
      "[" + @arguments.map { |a| inspect_val(a) }.join(", ") + "]"
    end

    private def inspect_val(v : ArgValue) : String
      case v
      when Hash(String | Symbol, ArgValue) then v.inspect
      when Array(ArgValue)                 then "[" + v.map { |x| inspect_val(x) }.join(", ") + "]"
      when String                          then v.inspect
      else                                      v.inspect
      end
    end
  end

  # Adapter interface: enqueue(job) (immediate) — scheduled jobs carry
  # their scheduled_at on the serialized record.
  module Adapter
    abstract def enqueue(record : EnqueuedJob) : Nil
  end

  class TestAdapter
    include Adapter

    def enqueued_jobs : Array(EnqueuedJob)
      @jobs ||= [] of EnqueuedJob
    end

    def enqueue(record : EnqueuedJob) : Nil
      enqueued_jobs << record
    end
  end
end
