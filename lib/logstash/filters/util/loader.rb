require_relative "read_only_database"

module LogStash module Filters module Util
  class Loader
    attr_reader :table, :query, :max_rows
    attr_reader :connection_string, :driver_library, :driver_class
    attr_reader :user, :password

    def initialize(table, options)
      @table = table.to_sym
      @options = options
      @valid = false
      @option_errors = []
      parse_options
    end

    def valid?
      @valid
    end

    def formatted_errors
      @option_errors.join(", ")
    end

    def build_remote_db
      @remote = ReadOnlyDatabase.new()
      @remote.connect(connection_string, driver_class, driver_library, user, password)
    end

    def fetch
      # db.dataset.limit(10).sql.sub(db.dataset.select.sql, "")
      @remote.query(query).take(max_rows)
    end

    # ------------------
    private

    def parse_options
      parsed = true
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
        @max_rows = 500
      end

      @driver_library = @options["jdbc_driver_library"]
      if @driver_library
        if !@driver_library.is_a?(String)
          @option_errors << "The 'jdbc_driver_library' option for '#{@table}' must be a string"
          parsed = false
        end
        if !File.exists?(@driver_library)
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
