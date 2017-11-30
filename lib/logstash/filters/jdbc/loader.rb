# encoding: utf-8
require_relative "validatable"
require_relative "db_object"
require_relative "read_only_database"
require "logstash/util/loggable"

module LogStash module Filters module Jdbc
  class Loader < Validatable
    include LogStash::Util::Loggable

    CONNECTION_ERROR_MSG = "Remote DB connection error when executing loader Jdbc query"

    attr_reader :id, :table, :temp_table, :query, :max_rows
    attr_reader :connection_string, :driver_library, :driver_class
    attr_reader :user, :password

    def build_remote_db
      @remote = ReadOnlyDatabase.create(connection_string, driver_class, driver_library, user, password)
    end

    def fetch
      @remote.connect(CONNECTION_ERROR_MSG)
      row_count = @remote.count(query)
      if row_count.zero?
        logger.warn? && logger.warn("Query returned no results", :query => query)
        return @remote.empty_record_set
      end
      if row_count > max_rows
        logger.warn? && logger.warn("Query returned more than max_rows results", :query => query, :count => row_count, :max_rows => max_rows)
        return @remote.empty_record_set
      end
      @remote.query(query)
    ensure
      @remote.disconnect(CONNECTION_ERROR_MSG)
    end

    def close
      @remote.disconnect(CONNECTION_ERROR_MSG)
    end

    private

    def pre_initialize(options)
      @table = options["local_table"]
    end

    def post_initialize
      if valid?
        @temp_table = "#{TEMP_TABLE_PREFIX}#{@table}".to_sym
        @table = @table.to_sym
      end
    end

    def parse_options
      parsed = true

      unless @table && @table.is_a?(String)
        @option_errors << "The options must include a 'local_table' string"
        parsed = false
      end

      @id = @options.fetch("id", @table)

      @query = @options["query"]
      unless @query && @query.is_a?(String)
        @option_errors << "The options for '#{@table}' must include a 'query' string"
        parsed = false
      end

      @max_rows = @options["max_rows"]
      if @max_rows
        if !@max_rows.respond_to?(:to_i)
          @option_errors << "The 'max_rows' option for '#{@table}' must be an integer"
          parsed = false
        else
          @max_rows = @max_rows.to_i
        end
      else
        @max_rows = 1_000_000
      end

      @driver_library = @options["jdbc_driver_library"]
      if @driver_library
        if !@driver_library.is_a?(String)
          @option_errors << "The 'jdbc_driver_library' option for '#{@table}' must be a string"
          parsed = false
        end
        if !::File.exists?(@driver_library)
          @option_errors << "The 'jdbc_driver_library' option for '#{@table}' must be a file that can be opened: #{driver_library}"
          parsed = false
        end
      end

      @driver_class = @options["jdbc_driver_class"]
      if @driver_class && !@driver_class.is_a?(String)
        @option_errors << "The 'jdbc_driver_class' option for '#{@table}' must be a string"
        parsed = false
      end

      @connection_string = @options["jdbc_connection_string"]
      if @connection_string && !@connection_string.is_a?(String)
        @option_errors << "The 'jdbc_connection_string' option for '#{@table}' must be a string"
        parsed = false
      end

      @user = @options["jdbc_user"]
      if @user && !@user.is_a?(String)
        @option_errors << "The 'jdbc_user' option for '#{@table}' must be a string"
        parsed = false
      end

      @password = @options["jdbc_password"]
      case @password
      when String
        @password = LogStash::Util::Password.new(@password)
      when LogStash::Util::Password, nil
        # this is OK
      else
        @option_errors << "The 'jdbc_password' option for '#{@table}' must be a string"
        parsed = false
      end
      @valid = parsed
    end
  end
end end end
