# encoding: utf-8

require "logstash/util/loggable"
require "concurrent"

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
      # we only need to check column validity once because if the config is creating a local
      # db table with column data type we can't coerce this is not transient (event dependent)
      # like invalid parameters is. The Java side does not really need to fill in the invalid columns each time.
      @column_validity_checked = Concurrent::AtomicBoolean.new
      # normally all the columns in the local db are coercible to Logstash Event data types
      # this flag becomes false in the very unlikely case that none of the columns are coercible
      # this is a error and the defaults should be used
      @some_valid_columns = true
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

    # @param [ReadWriteDatabase] local, the local derby database
    # @param [LogStash::Event] event, the current event being enhanced by this lookup
    # @return [Boolean] returns true when the event contains the looked up fields/values or the default value was applied. False in all other cases
    # meaning that only if all lookups enhance successfully then `filter_matched` is called
    def enhance(local, event)
      # LookupFailures API (JRuby extension)
      # successful?,  no errors, event was updated with looked up fields/values
      # invalid_parameters, array of invalid parameters
      # checking_columns?, has valid/invalid columns been collected for this instance
      # check_columns, we only need to do this once
      # all_columns, array of columns attempted to convert to Ruby, if collected, this array is converted from a java HashSet to ruby on demand
      # invalid_columns, array of unacceptable column datatype messages, if collected, this array is converted from a java HashSet to ruby on demand
      # invalid_id_for_lookup?, was the id supplied unregistered? Very unlikely.
      # any_invalid_columns?, are there any unacceptable columns in the resultset?
      # any_invalid_parameters?, are there any parameters that did not...
      #      A) interpolate well
      #      B) resolve to nil, array or hash,
      #      C) have a type we don't coerce to a SQL type (unlikely, as we cover what Valuefier handles)
      begin
        result = if @column_validity_checked.true?
                  LookupFailures.new
                else
                  @column_validity_checked.make_true
                  LookupFailures.new.check_columns
                end
        local.fetch_with_lock(@id, event, result) # updates the instance of LookupFailures (see comments above), defined in JRuby extension
        return true if result.successful?
      rescue StandardError => e
        # a SQLException re-thrown as as a StandardError, the cause and backtrace has been logged already in Java
        # this error holds no meaningful info.
        tag_failure(event)
        return tag_and_set_default(event)
      end

      if result.any_invalid_parameters?
        logger.warn("Parameters for the statement cannot be prepared, interpolation may have failed or the value might be nil, array or hash", "parameters" => result.invalid_parameters, "event" => event.to_hash)
        # if this is a checking columns run and there are invalid parameters then the prepared statement is not
        # even executed so columns cannot validated this time - need to check columns again on next event.
        @column_validity_checked.make_false if result.checking_columns?
      else
        if result.checking_columns? && result.any_invalid_columns?
          # this means that a local db schema has been created with one or more datatypes
          # that we can't coerce into acceptable Event datatypes. Other field/values are added to the Event.
          invalid_columns = result.invalid_columns
          all_columns = result.all_columns
          @some_valid_columns = all_columns.size > invalid_columns.size
          logger.error("The statement is returning data types that cannot be stored in an event", "invalid columns" => invalid_columns, "all columns" => all_columns)
        end
      end

      if result.invalid_id_for_lookup?
        logger.error("This local lookup has not been registered correctly, this is abnormal", "local_lookup id" => @id)
      end

      tag_failure(event)

      if @some_valid_columns
        # Some fields are filled in and so this is tagged failure
        # but not a use the defaults failure
        return true
      end

      tag_and_set_default(event)
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
