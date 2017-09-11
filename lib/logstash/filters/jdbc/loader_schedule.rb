require_relative "validatable"

module LogStash module Filters module Jdbc
  class LoaderSchedule < Validatable
    attr_reader :schedule_frequency, :loader_schedule

    private

    def post_initialize
      if valid?
        if @cronline.seconds.is_a?(Set)
          @schedule_frequency = 0.3
        else
          @schedule_frequency = 30
        end
      end
    end

    def parse_options
      parsed = true

      @loader_schedule = @options

      unless @loader_schedule.is_a?(String)
        @option_errors << "The loader_schedule option must be a string"
        parsed = false
      end

      begin
        @cronline = Rufus::Scheduler::CronLine.new(@loader_schedule)
      rescue => e
        @option_errors << "The loader_schedule option is invalid: #{e.message}"
        parsed = false
      end

      @valid = parsed
    end
  end
end end end
