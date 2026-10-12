# ActiveJob::Cron — 5-field cron expressions with the semantics of
# fugit (what solid_queue uses for recurring tasks), verified against
# the oracle captured from fugit 1.12.1 (oracle/solid_queue_cases.rb,
# cases sq_cron_next / sq_cron_prev):
#
#   minute hour day-of-month month day-of-week
#
# - `*`, lists `a,b`, ranges `a-b`, steps `*/n` and `a-b/n`
# - month/day names (jan..dec, sun..sat), 7 == sunday == 0
# - @daily @hourly @weekly @monthly @yearly @annually @midnight
# - next_time: strictly AFTER `from`; previous_time: at or before.
# - day-of-month/day-of-week: when both are restricted, a day matching
#   EITHER qualifies (standard cron semantics, fugit-compatible).

module ActiveJob
  class Cron
    class ParseError < Exception
    end

    getter minutes : Array(Int32)
    getter hours : Array(Int32)
    getter days : Array(Int32)
    getter months : Array(Int32)
    getter dows : Array(Int32)
    getter? dom_star : Bool
    getter? dow_star : Bool

    def initialize(@minutes, @hours, @days, @months, @dows,
                   @dom_star, @dow_star)
    end

    NAMES_DOW = {"sun" => 0, "mon" => 1, "tue" => 2, "wed" => 3,
                 "thu" => 4, "fri" => 5, "sat" => 6}
    NAMES_MONTH = {"jan" => 1, "feb" => 2, "mar" => 3, "apr" => 4,
                   "may" => 5, "jun" => 6, "jul" => 7, "aug" => 8,
                   "sep" => 9, "oct" => 10, "nov" => 11, "dec" => 12}

    ALIASES = {
      "@yearly"    => "0 0 1 1 *",
      "@annually"  => "0 0 1 1 *",
      "@monthly"   => "0 0 1 * *",
      "@weekly"    => "0 0 * * 0",
      "@daily"     => "0 0 * * *",
      "@midnight"  => "0 0 * * *",
      "@hourly"    => "0 * * * *",
    }

    def self.parse(expr : String) : self?
      expr = expr.strip
      expr = ALIASES[expr]? || expr
      return nil unless expr =~ /\A[\w\-*,\/]+( [\w\-*,\/]+){4}\z/
      min, hour, dom, mon, dow = expr.split
      begin
        new(
          field(min, 0, 59),
          field(hour, 0, 23),
          field(dom, 1, 31),
          field(mon, 1, 12, NAMES_MONTH),
          field(dow, 0, 7, NAMES_DOW).map { |d| d == 7 ? 0 : d }.uniq.sort,
          star?(dom), star?(dow))
      rescue err : ParseError
        nil
      end
    end

    def initialize(@minutes : Array(Int32), @hours : Array(Int32),
                   @days : Array(Int32), @months : Array(Int32),
                   @dows : Array(Int32), @dom_star : Bool, @dow_star : Bool)
    end

    # Strict parser: raises on invalid input (parse? returns nil instead).
    def self.parse!(expr : String) : self
      parsed = parse(expr)
      raise ParseError.new("invalid cron expression: #{expr}") unless parsed
      parsed
    end

    # --- matching -------------------------------------------------------

    def matches?(t : Time) : Bool
      return false unless months.includes?(t.month)
      cron_dow = t.day_of_week.value % 7 # Mon=1..Sun=7 → 0=sun
      if dom_star? && dow_star?
        day_ok = true
      elsif dom_star?
        day_ok = dows.includes?(cron_dow)
      elsif dow_star?
        day_ok = days.includes?(t.day)
      else
        day_ok = days.includes?(t.day) || dows.includes?(cron_dow)
      end
      return false unless day_ok
      hours.includes?(t.hour) && minutes.includes?(t.minute)
    end

    # --- next / previous (fugit semantics) --------------------------------

    def next_time(from : Time) : Time
      t = floor_to_minute(from)
      # strictly after: advance one minute when `from` itself is a match
      t += 1.minute if t == floor_to_minute(from) && matches?(t)
      t += 1.minute if t <= from

      547.times do # ~18 months of day candidates
        if day_matches?(t)
          if hit = first_hit_in_day(t.year, t.month, t.day,
                                    t.hour, t.minute, after: true)
            return hit
          end
        end
        t = start_of_next_day(t)
      end
      raise ParseError.new("no next occurrence within 18 months")
    end

    def previous_time(from : Time) : Time
      t = floor_to_minute(from)

      547.times do
        if day_matches?(t)
          if hit = first_hit_in_day(t.year, t.month, t.day,
                                    t.hour, t.minute, after: false)
            return hit
          end
        end
        t = start_of_day(t) - 1.minute
      end
      raise ParseError.new("no previous occurrence within 18 months")
    end

    def to_s : String
      "#{fmt(minutes)} #{fmt(hours)} #{star_or(days, dom_star?)} " \
        "#{fmt(months)} #{star_or(dows, dow_star?)}"
    end

    # --- internals --------------------------------------------------------

    private def fmt(values : Array(Int32)) : String
      return "*" if full_range?(values)
      values.join(",")
    end

    private def star_or(values : Array(Int32), star : Bool) : String
      star ? "*" : fmt(values)
    end

    private def full_range?(values) : Bool
      false # conservative; only used for display
    end

    private def day_matches?(t : Time) : Bool
      return false unless months.includes?(t.month)
      cron_dow = t.day_of_week.value % 7
      if dom_star? && dow_star?
        true
      elsif dom_star?
        dows.includes?(cron_dow)
      elsif dow_star?
        days.includes?(t.day)
      else
        days.includes?(t.day) || dows.includes?(cron_dow)
      end
    end

    # First (after: true) or last (after: false) matching minute within
    # the given day, constrained to be >= (or <=) h:m of that same day.
    private def first_hit_in_day(y, m, d, h, min, after : Bool) : Time?
      candidates = [] of Time
      hours.each do |hh|
        minutes.each do |mm|
          t = Time.utc(y, m, d, hh, mm)
          candidates << t
        end
      end
      return nil if candidates.empty?
      anchor = Time.utc(y, m, d, h, min)
      if after
        candidates.select { |t| t >= anchor }.min?
      else
        candidates.select { |t| t <= anchor }.max?
      end
    end

    private def floor_to_minute(t : Time) : Time
      Time.utc(t.year, t.month, t.day, t.hour, t.minute)
    end

    private def start_of_day(t : Time) : Time
      Time.utc(t.year, t.month, t.day)
    end

    private def start_of_next_day(t : Time) : Time
      start_of_day(t) + 1.day
    end

    # --- field parsing ------------------------------------------------------

    private def self.star?(field : String) : Bool
      field == "*" || field.starts_with?("*/")
    end

    private def self.field(spec : String, min : Int32, max : Int32,
                           names = {} of String => Int32) : Array(Int32)
      values = [] of Int32
      spec.split(',').each do |part|
        range = part
        step = 1
        if idx = part.index('/')
          range = part[0...idx]
          step = part[(idx + 1)..].to_i
        end
        raise ParseError.new("bad step #{part}") if step < 1
        lo, hi =
          if range == "*"
            {min, max}
          elsif range.includes?('-')
            a, b = range.split('-', 2)
            {value(a, min, names), value(b, min, names)}
          else
            v = value(range, min, names)
            step_given = part.includes?('/')
            {v, step_given ? max : v}
          end
        raise ParseError.new("bad range #{part}") if lo > hi
        lo.step(to: hi, by: step) { |v| values << v if v >= min && v <= max }
      end
      raise ParseError.new("empty field #{spec}") if values.empty?
      values.uniq.sort
    end

    private def self.value(token : String, min : Int32,
                           names : Hash(String, Int32)) : Int32
      if v = names[token]?
        return v
      end
      token.to_i? || raise ParseError.new("bad value #{token}")
    end
  end
end
