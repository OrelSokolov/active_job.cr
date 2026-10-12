# Argument (de)serialization — mirrors ActiveJob's serialized format
# byte-for-byte: symbol hash keys become strings plus `_aj_symbol_keys`,
# Date/Time become `_aj_serialized` envelopes, primitives pass as-is.

module ActiveJob
  class SerializationError < Exception
  end

  # Recursively-typed job argument value. Declared inside the module —
  # recursive aliases at top level crash the Crystal compiler.
  alias ArgValue = Nil | Bool | Int32 | Int64 | Float64 | String | Symbol |
                   Array(ArgValue) | Hash(String | Symbol, ArgValue) |
                   Date | Time

  DATE_SERIALIZER    = "ActiveJob::Serializers::DateSerializer"
  TIME_SERIALIZER    = "ActiveJob::Serializers::TimeSerializer"
  SYM_KEYS_FIELD     = "_aj_symbol_keys"
  SERIALIZED_FIELD   = "_aj_serialized"

  module Serialization
    # Runtime conversion of a job argument into the ArgValue union.
    # Containers are rebuilt element-wise: Crystal generics are
    # invariant, so Array(Int32) cannot be cast to Array(ArgValue).
    def self.to_arg(a) : ArgValue
      case a
      when Nil, Bool, Int32, Int64, Float64, String, Symbol, Date, Time then a
      when Array
        arr = [] of ArgValue
        a.each { |x| arr << to_arg(x) }
        arr
      when Hash
        h = {} of String | Symbol => ArgValue
        a.each do |k, v|
          key = case k
                when String, Symbol then k
                else k.to_s
                end
          h[key] = to_arg(v)
        end
        h
      else
        raise SerializationError.new("Unsupported argument type: #{a.class}")
      end
    end

    def self.serialize_arg(arg : ArgValue) : ArgValue
      case arg
      when Nil, Bool, Int32, Int64, Float64, String, Symbol then arg
      when Array
        arg.map { |v| serialize_arg(v.as(ArgValue)).as(ArgValue) }
      when Hash
        out = {} of String | Symbol => ArgValue
        symbol_keys = [] of String
        arg.each do |k, v|
          if k.is_a?(Symbol)
            symbol_keys << k.to_s
            out[k.to_s] = serialize_arg(v)
          else
            out[k] = serialize_arg(v)
          end
        end
        out[SYM_KEYS_FIELD] = symbol_keys.map(&.as(ArgValue)) unless symbol_keys.empty?
        out
      when Date
        env = {} of String | Symbol => ArgValue
        env[SERIALIZED_FIELD] = DATE_SERIALIZER
        env["value"] = arg.to_s
        env
      when Time
        env = {} of String | Symbol => ArgValue
        env[SERIALIZED_FIELD] = TIME_SERIALIZER
        env["value"] = iso8601_ns(arg)
        env
      else
        raise SerializationError.new("Unsupported argument type: #{arg.class}")
      end
    end

    def self.deserialize_arg(arg : ArgValue) : ArgValue
      case arg
      when Array
        arg.map { |v| deserialize_arg(v.as(ArgValue)).as(ArgValue) }
      when Hash
        if arg[SERIALIZED_FIELD]? == DATE_SERIALIZER
          Date.parse(arg["value"].as(String))
        elsif arg[SERIALIZED_FIELD]? == TIME_SERIALIZER
          Time.parse_iso8601(arg["value"].as(String))
        else
          out = {} of String | Symbol => ArgValue
          arg.each do |k, v|
            out[k] = deserialize_arg(v) unless k == SYM_KEYS_FIELD
          end
          out
        end
      else
        arg
      end
    end

    # "2026-01-01T12:00:00.000000000Z" — Ruby Time#iso8601(9) format.
    def self.iso8601_ns(t : Time) : String
      d = t.year.to_s.rjust(4, '0') + '-' + t.month.to_s.rjust(2, '0') + '-' + t.day.to_s.rjust(2, '0')
      tm = t.hour.to_s.rjust(2, '0') + ':' + t.minute.to_s.rjust(2, '0') + ':' + t.second.to_s.rjust(2, '0')
      d + 'T' + tm + '.' + t.nanosecond.to_s.rjust(9, '0') + "Z"
    end
  end
end
