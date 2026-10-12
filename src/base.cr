# ActiveJob::Base — the job DSL and execution core.
#
# Design notes vs Rails:
# - DSL macros (queue_as/retry_on/discard_on/callbacks) generate
#   per-class methods: Crystal class variables are shared across the
#   hierarchy, so per-job rules must be compile-time.
# - perform_now semantics mirror Rails 7.1+/8.x (oracle-verified): the
#   executions counter increments BEFORE perform; on failure
#   retry_on re-enqueues and perform_now RETURNS the caught exception;
#   discard_on returns it too; only exhausted attempts re-raise.

module ActiveJob
  JOBS = [] of Nil # compile-time registry, filled via `inherited`

  abstract class Base
    macro inherited
      {% ::ActiveJob::JOBS << @type %}
    end

    getter job_id : String = UUID.random.to_s

    # Last perform's return value (perform_now returns it). Limited to
    # scalars — Crystal needs a closed type for the ivar; documented
    # deviation from Ruby's "anything".
    property __perform_result : String | Int32 | Int64 | Float64 | Bool | Nil = nil
    getter? successfully_enqueued : Bool = false
    property executions : Int32 = 0
    property exception_executions : Hash(String, Int32) = {} of String => Int32
    property arguments : Array(ArgValue) = [] of ArgValue
    property scheduled_at : Time? = nil
    property provider_job_id : String? = nil

    def initialize(*args)
      converted = [] of ArgValue
      args.each { |a| converted << Serialization.to_arg(a) }
      @arguments = converted
      @queue_name = self.class.default_queue_name
      @priority = self.class.default_priority
    end

    # Child classes define `perform` with whatever signature they need
    # (no abstract declaration — signatures vary per job).

    def queue_name : String
      @queue_name
    end

    def queue_name=(name : String)
      @queue_name = name
    end

    def priority : Int32?
      @priority
    end

    def priority=(p : Int32?)
      @priority = p
    end

    # --- DSL macros ---------------------------------------------------

    macro queue_as(name)
      def self.default_queue_name : String
        {{ name.id.stringify }}
      end
    end

    macro queue_with_priority(value)
      def self.default_priority : Int32?
        {{ value }}
      end
    end

    macro retry_on(klass, attempts = 3, wait = 3.seconds)
      def self.__retry_rule : Tuple(Exception.class, Int32, Time::Span)?
        { {{ klass }}, {{ attempts }}, {{ wait }} }
      end
    end

    macro discard_on(klass)
      def self.__discard_classes : Array(Exception.class)
        [ {{ klass }} ] of Exception.class
      end
    end

    macro before_perform(&block)
      def __cb_before_perform : Nil
        {{ block.body }}
      end
    end

    macro after_perform(&block)
      def __cb_after_perform : Nil
        {{ block.body }}
      end
    end

    macro around_perform(&block)
      def __cb_around_perform(inner : Proc(Nil)) : Nil
        {% unless block.args.empty? %}{{ block.args[0].id }} = self{% end %}
        {% if block.args.size > 1 %}{{ block.args[1].id }} = inner{% end %}
        {{ block.body }}
      end
    end

    macro before_enqueue(&block)
      def __cb_before_enqueue : Nil
        {{ block.body }}
      end
    end

    macro after_enqueue(&block)
      def __cb_after_enqueue : Nil
        {{ block.body }}
      end
    end

    macro around_enqueue(&block)
      def __cb_around_enqueue(inner : Proc(Nil)) : Nil
        {% unless block.args.empty? %}{{ block.args[0].id }} = self{% end %}
        {% if block.args.size > 1 %}{{ block.args[1].id }} = inner{% end %}
        {{ block.body }}
      end
    end

    # DSL defaults (overridden by the macros above).
    def self.default_queue_name : String
      "default"
    end

    def self.default_priority : Int32?
      nil
    end

    def self.__retry_rule : Tuple(Exception.class, Int32, Time::Span)?
      nil
    end

    def self.__discard_classes : Array(Exception.class)
      [] of Exception.class
    end

    # No-op callback defaults.
    def __cb_before_perform : Nil; end

    def __cb_after_perform : Nil; end

    def __cb_around_perform(inner : Proc(Nil)) : Nil
      inner.call
    end

    def __cb_before_enqueue : Nil; end

    def __cb_after_enqueue : Nil; end

    def __cb_around_enqueue(inner : Proc(Nil)) : Nil
      inner.call
    end

    # --- execution ----------------------------------------------------

    def self.perform_now(*args)
      job = new(*args)
      job.__execute_now
    end

    # Instance form: run this (possibly deserialized) job with its own
    # stored arguments — `Base.deserialize(data).perform_now`.
    def perform_now
      __execute_now
    end

    def __execute_now
      self.executions += 1
      __cb_before_perform
      result : String | Int32 | Int64 | Float64 | Bool | Nil = nil
      __cb_around_perform(->{
        result = __perform_call
        __cb_after_perform
      })
      result
    rescue ex : Exception
      __handle_failure(ex)
    end

    # Splat of the stored arguments into the job's perform. Crystal
    # can't splat an Array into a def, and it type-checks EVERY branch,
    # so valid call arities are generated from the job's own perform
    # signature (min required .. max incl. defaults) at instantiation
    # time. Limitation: perform must be defined in each job class.
    # `__perform_call` is generated per subclass at the end of the
    # program (see the top-level `finished` macro below): it must be
    # compiled against each job's own perform signature, but subclass
    # methods aren't visible until the class body is complete.
    private def __perform_call
      raise ArgumentError.new("Wrong number of arguments for #{self.class}#perform: #{arguments.size}")
    end

    private def __handle_failure(ex : Exception)
      return ex if self.class.__discard_classes.any?(&.== ex.class)
      if rule = self.class.__retry_rule
        klass, attempts, wait = rule
        if ex.class == klass
          key = "[#{ex.class.name}]"
          exception_executions[key] = (exception_executions[key]? || 0) + 1
          raise ex if executions >= attempts
          # schedule the retry (default wait mirrors Rails' 3s default)
          __enqueue(Time.utc + wait)
          return ex
        end
      end
      raise ex
    end

    # --- enqueue --------------------------------------------------------

    def self.perform_later(*args)
      job = new(*args)
      job.__enqueue(nil)
      job
    end

    def __enqueue(scheduled : Time?) : Nil
      self.scheduled_at = scheduled
      __cb_before_enqueue
      __cb_around_enqueue(->{
        ActiveJob.adapter.enqueue(__build_record)
        @successfully_enqueued = true
        __cb_after_enqueue
      })
    end

    def __build_record : EnqueuedJob
      EnqueuedJob.new(
        self.class.name, queue_name, priority,
        arguments.map { |a| Serialization.serialize_arg(a).as(ArgValue) },
        executions, exception_executions, scheduled_at, self.class)
    end

    # --- (de)serialization ----------------------------------------------

    def serialize : Hash(String, ArgValue)
      ee = {} of String | Symbol => ArgValue
      exception_executions.each { |k, v| ee[k] = v }
      {
        "job_class"            => self.class.name,
        "job_id"               => job_id,
        "provider_job_id"      => provider_job_id,
        "queue_name"           => queue_name,
        "priority"             => priority,
        "arguments"            => arguments.map { |a| Serialization.serialize_arg(a).as(ArgValue) },
        "executions"           => executions,
        "exception_executions" => ee,
        "locale"               => "en",
        "timezone"             => nil,
        "enqueued_at"          => Serialization.iso8601_ns(Time.utc),
        "scheduled_at"         => scheduled_at.try { |t| Serialization.iso8601_ns(t) },
      } of String => ArgValue
    end

    # Job-class dispatch by name. Macro `for` can't open `case`
    # branches and macro variables don't persist inside def bodies —
    # so the codegen lives in a macro. The generated lines reference
    # the caller's `name`/`args` locals textually and assign `job`.
    private macro __gen_job_dispatch
      {% for j in ::ActiveJob::JOBS %}
        job = {{ j.id }}.new if name == {{ j.stringify }} && args.size == 0
        {% for k in 1..4 %}
          job = {{ j.id }}.new({% for i in 0...k %}args[{{ i }}], {% end %}) if name == {{ j.stringify }} && args.size == {{ k }}
        {% end %}
      {% end %}
    end

    # Job-class lookup by name (compile-time registry) — used by
    # recurring tasks and anything resolving class_name strings.
    def self.find_job_class(name : String) : Base.class | Nil
      {% for j in ::ActiveJob::JOBS %}
        return {{ j.id }} if name == {{ j.stringify }}
      {% end %}
      nil
    end

    def self.deserialize(h : Hash(String, ArgValue)) : Base
      args = (h["arguments"]? || [] of ArgValue).as(Array(ArgValue))
        .map { |v| Serialization.deserialize_arg(v).as(ArgValue) }
      name = h["job_class"].as(String)
      job = nil
      __gen_job_dispatch
      raise ArgumentError.new("Unknown job class #{name}") unless job
      job = job.not_nil!
      job.executions = (h["executions"]? || 0).as(Int32)
      if ee = h["exception_executions"]?
        hash = {} of String => Int32
        ee.as(Hash(String | Symbol, ArgValue)).each do |k, v|
          hash[k.to_s] = v.as(Int32)
        end
        job.exception_executions = hash
      end
      if s = h["scheduled_at"]?.try(&.as?(String))
        job.scheduled_at = Time.parse_iso8601(s)
      end
      job
    end

    # set(queue:, wait:, wait_until:, priority:) — like Rails' JobSetter.
    def self.set(*, queue : String? = nil, priority : Int32? = nil,
                 wait : ActiveSupport::Duration | Time::Span? = nil, wait_until : Time? = nil) : JobSetter
      JobSetter.new(self, queue, priority, wait, wait_until)
    end
  end

  class JobSetter
    def initialize(@klass : Base.class, @queue : String?, @priority : Int32?,
                   @wait : ActiveSupport::Duration | Time::Span?, @wait_until : Time?)
    end

    def perform_later(*args) : Base
      job = @klass.new(*args)
      job.queue_name = @queue.not_nil! if @queue
      job.priority = @priority.not_nil! if @priority
      scheduled = @wait_until || @wait.try { |d| Time.utc + d }
      job.__enqueue(scheduled)
      job
    end
  end
end

# End-of-program codegen: per-subclass perform dispatch (arity is only
# known once the whole class body — including perform — is parsed).
macro finished
  {% for j in ::ActiveJob::JOBS %}
    {% has_perform = false %}
    {% req = 0 %}
    {% max = 0 %}
    {% for m in j.methods %}
      {% if m.name.stringify == "perform" %}
        {% has_perform = true %}
        {% max = m.args.size %}
        {% cnt = 0 %}
        {% for a in m.args %}
          {% cnt += 1 if a.default_value.is_a?(Nop) %}
        {% end %}
        {% req = cnt %}
      {% end %}
    {% end %}
    {% if has_perform %}
      class {{ j.id }}
        private def __perform_call
          n = arguments.size
          {% for i in 0..4 %}
            {% if i >= req && i <= max %}
              return perform({% for k in 0...i %}arguments[{{ k }}], {% end %}) if n == {{ i }}
            {% end %}
          {% end %}
          raise ArgumentError.new("Wrong number of arguments for #{self.class}#perform: #{n}")
        end
      end
    {% end %}
  {% end %}
end
