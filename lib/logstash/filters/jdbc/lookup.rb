require_relative "lookup_result"
require "logstash/util/loggable"

module LogStash module Filters module Jdbc
  class Lookup
    include LogStash::Util::Loggable

    class Sprintfier
      def initialize(param)
        @param = param
      end

      def fetch(event)
        event.sprintf(@param)
      end
    end

    class Getfier
      def initialize(param)
        @param = param
      end

      def fetch(event)
        event.get(@param)
      end
    end

    attr_reader :target, :query, :parameters

    def self.validate(array_of_options)
      if !array_of_options.is_a?(Array)
        return "The options must be an Array"
      end
      errors = []
      array_of_options.each_with_index do |options, i|
        instance = new(options, {}, "lookup-#{i.next}")
        unless instance.valid?
          errors << instance.formatted_errors
        end
      end
      return nil if errors.empty?
      errors.join("; ")
    end

    def initialize(options, globals, default_id)
      @target = options["target"]
      @id = target || default_id
      @options = options
      @globals = globals
      @valid = false
      @option_errors = []
      @default_array = nil
      parse_options
    end

    def valid?
      @valid
    end

    def formatted_errors
      @option_errors.join(", ")
    end

    def enhance(local, event)
      result = fetch(local, event) # should return a LookupResult

      if result.failed?
        tag_failure(event)
      end

      if result.empty? && @use_default
        tag_default(event)
        process_event(event, @default_array)
        return
      end
      process_event(event, result.payload)
    end

    private

    def tag_failure(event)
      @tag_on_failure.each do |tag|
        event.tag(tag)
      end
    end

    def tag_default(event)
      @tag_on_default_use.each do |tag|
        event.tag(tag)
      end
    end

    def fetch(local, event)
      result = LookupResult.new()
      params = prepare_parameters_from_event(event)
      begin
        logger.debug? && logger.debug("Executing Jdbc query", :statement => query, :parameters => params)
        local.fetch(query, params).each do |row|
          stringified = row.inject({}){|hash,(k,v)| hash[k.to_s] = v; hash} #Stringify row keys
          result.push(stringified)
        end
      rescue ::Sequel::Error => e
        # all sequel errors are a subclass of this, let all other standard or runtime errors bubble up
        result.failed!
        logger.warn? && logger.warn("Exception when executing Jdbc query", :exception => e.message, :backtrace => e.backtrace.take(8))
      end
      # if either of: no records or a Sequel exception occurs the payload is
      # empty and the default can be substituted later.
      result
    end

    def process_event(event, value)
      # use deep clone here so other filter function don't taint the payload by reference
      event.set(@target, ::LogStash::Util.deep_clone(value))
    end

    def prepare_parameters_from_event(event)
      @symbol_parameters.inject({}) do |hash,(k,v)|
        value = v.fetch(event)
        hash[k] = value.is_a?(::LogStash::Timestamp) ? value.time : value
        hash
      end
    end

    def sprintf_or_get(v)
      v.match(/%{([^}]+)}/) ? Sprintfier.new(v) : Getfier.new(v)
    end

    def parse_options
      parsed = true
      @query = @options["query"]
      unless @query && @query.is_a?(String)
        @option_errors << "The options for '#{@id}' must include a 'query' string"
        parsed = false
      end

      @parameters = @options["parameters"]
      if @parameters
        if !@parameters.is_a?(Hash)
          @option_errors << "The 'parameters' option for '#{@id}' must be a Hash"
          parsed = false
        else
          # this is done once per lookup at start, i.e. Sprintfier.new et.al is done once.
          @symbol_parameters = @parameters.inject({}) {|hash,(k,v)| hash[k.to_sym] = sprintf_or_get(v) ; hash }
        end
      end

      default_hash = @options["default_hash"]
      if default_hash && !default_hash.empty?
        @default_array = [default_hash]
      end

      @use_default = !!(@default_array)

      @tag_on_failure = @options["tag_on_failure"] || @globals["tag_on_failure"] || []
      @tag_on_default_use = @options["tag_on_default_use"] || @globals["tag_on_default_use"] || []

      @valid = parsed
    end
  end
end end end
