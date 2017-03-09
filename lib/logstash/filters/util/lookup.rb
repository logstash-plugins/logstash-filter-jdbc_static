require_relative "lookup_result"

module LogStash module Filters module Util
  class Lookup
    attr_reader :target, :query, :parameters

    def initialize(target, options, globals, logger)
      @target = target
      @options = options
      @globals = globals
      @logger = logger
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

    # ------------------
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
        @logger.debug? && @logger.debug("Executing JDBC query", :statement => query, :parameters => params)
        local.fetch(query, params).each do |row|
          result.push row.inject({}){|hash,(k,v)| hash[k.to_s] = v; hash} #Stringify row keys
        end
      rescue ::Sequel::Error => e
        # all sequel errors are a subclass of this, let all other standard or runtime errors bubble up
        result.failed!
        @logger.warn? && @logger.warn("Exception when executing JDBC query", :exception => e)
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
        value = event.get(event.sprintf(v))
        hash[k] = value.is_a?(::LogStash::Timestamp) ? value.time : value
        hash
      end
    end

    def parse_options
      parsed = true
      @query = @options["query"]
      unless @query && @query.is_a?(String)
        @option_errors << "The options for '#{@target}' must include a 'query' string"
        parsed = false
      end

      @parameters = @options["parameters"]
      if @parameters
        if !@parameters.is_a?(Hash)
          @option_errors << "The 'parameters' option for '#{@target}' must be a Hash"
          parsed = false
        else
          @symbol_parameters = @parameters.inject({}) {|hash,(k,v)| hash[k.to_sym] = v ; hash }
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
