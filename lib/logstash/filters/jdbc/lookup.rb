# encoding: utf-8

require "logstash/util/loggable"

module LogStash module Filters module Jdbc
  class Lookup
    include LogStash::Util::Loggable

    def self.find_validation_errors(array_of_options)
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

    attr_reader :id, :target, :query, :parameters

    def initialize(options, globals, default_id)
      @id = options["id"] || default_id
      @target = options["target"]
      @id_used_as_target = @target.nil?
      if @id_used_as_target
        @target = @id
      end
      @options = options
      @globals = globals
      @valid = false
      @option_errors = []
      @default_result = nil
      parse_options
    end

    def add_lookup_to_fetcher(local)
      @fetcher_opts ||= {
      "id" => @id,
      "target" => @id_used_as_target ? @id : @target,
      "query" => @query,
      "parameters" => @parameters
      }
      local.add_lookup(@fetcher_opts)
    end

    def id_used_as_target?
      @id_used_as_target
    end

    def valid?
      @valid
    end

    def formatted_errors
      @option_errors.join(", ")
    end

    # LookupFailures API
    # invalid_parameters # array of invalid parameters
    # invalid_columns # array of unacceptable column datatype messages, this array is converted from a java HashSet to ruby on demand
    # invalid_id_for_lookup? # was the id supplied unregistered?
    # any_invalid_columns? # are there any unacceptable columns in the resultset?
    # any_invalid_parameters? # are there any parameters that did not
    #                           A) interpolate well, B) resolve to nil, array or hash,
    #                           C) have a type we don't coerce to a SQL type (unlikely, as we cover what Valuefier handles)
    def enhance(local, event)
      begin
        result = local.fetch_with_lock(@id, event) # returns an instance of LookupFailures, defined in JRuby extension
      rescue StandardError
        # a SQLException re-thrown as as a StandardError, the cause and backtrace has been logged already in Java
        # this error holds no meaningful info.
        tag_failure(event)
        return tag_and_set_default(event)
      end

      if result.invalid_id_for_lookup?
        logger.error("This local lookup has not been registered correctly, this is abnormal", "local_lookup id" => @id)
        tag_failure(event)
        return tag_and_set_default(event)
      end

      if result.any_invalid_parameters?
        logger.warn("Parameters for the statement cannot be prepared, interpolation may have failed or the value might be nil, array or hash", "parameters" => result.invalid_parameters, "event" => event.to_hash)
        tag_failure(event)
        return tag_and_set_default(event)
      end
      # this means that a local db schema has been created with one or more datatypes
      # that we can't coerce into acceptable Event datatypes. Other field/values are added to the Event.
      # Should we tag here or WARN only?
      if result.any_invalid_columns?
        logger.warn("The statement is returning datatypes that cannot be stored in an event", "invalid columns" => result.invalid_columns)
        tag_failure(event)
        return false
      end
      true
    end

    private

    def tag_and_set_default(event)
      if @use_default
        event.set(@target, ::LogStash::Util.deep_clone(@default_hash))
        tag_default(event)
        true
      else
        false
      end
    end

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

    def parse_options
      @query = @options["query"]
      unless @query && @query.is_a?(String)
        @option_errors << "The options for '#{@id}' must include a 'query' string"
      end

      @parameters = @options["parameters"]
      @parameters_specified = false
      if @parameters
        if !@parameters.is_a?(Hash)
          @option_errors << "The 'parameters' option for '#{@id}' must be a Hash"
        else
          @parameters_specified = !@parameters.empty?
        end
      end

      @default_hash = @options["default_hash"]

      @use_default = !@default_hash.nil?

      @tag_on_failure = @options["tag_on_failure"] || @globals["tag_on_failure"] || []
      @tag_on_default_use = @options["tag_on_default_use"] || @globals["tag_on_default_use"] || []

      @valid = @option_errors.empty?
    end
  end
end end end
