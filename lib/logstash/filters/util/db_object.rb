require_relative "validatable"
require_relative "column"

module LogStash module Filters module Util

  TEMP_TABLE_PREFIX = "temp_".freeze

  class DbObject < Validatable
    #   {type => "table", name => "servers", columns => [["ip", "text"], ["name", "text"], ["location", "text"]]},
    #   {type => "index", name => "servers_idx", table => "servers", columns => ["ip"]}


    attr_reader :type, :name, :table, :columns, :builder

    def build(db)
      return unless valid?
      if index?
        IndexBuilder.new(@name, @columns, @table).build(db)
      else
        TableBuilder.new(@name, @columns).build(db)
      end
    end

    class TableBuilder
      attr_reader :name, :columns
      def initialize(name, columns)
        @name, @columns = name, columns
      end
      def build(db)
        schema_gen = db.create_table_generator()
        @columns.each {|col| schema_gen.column(col.name, col.datatype)}
        options = {:generator => schema_gen}
        db.create_table(@name, options)
      end
    end

    class IndexBuilder
      attr_reader :name, :columns, :table
      def initialize(name, columns, table)
        @name, @columns, @table = name, columns, table
      end
      def build(db)
        db.add_index(@table, @columns, :name => @name)
      end
    end

    def <=>(other)
      return -1 if table? && other.index?
      return 1 if index? == other.table?
      0
    end

    def index?
      @type == "index"
    end

    def table?
      @type == "table"
    end

    def as_temp_table_opts
      return {} if !table?
      {"name" => "#{TEMP_TABLE_PREFIX}#{@name}", "type" => @type, "columns" => @columns.map(&:to_array)}
    end

    # ------------------
    private

    def post_initialize
      if valid?
        @name = @name.to_sym
        @table = @table.to_sym if !table?
      end
    end

    def parse_options
      parsed = true
      @name = @options["name"]
      unless @name && @name.is_a?(String)
        @option_errors << "The options must include a 'name' string"
        parsed = false
      end

      @table = @options["table"]
      if @table && !@table.is_a?(String)
        @option_errors << "The table option for '#{@name}' must be a string"
        parsed = false
      end

      @type = @options["type"]
      unless @type && @type.is_a?(String)
        @option_errors << "The options for '#{@name}' must include a 'type' string"
        parsed = false
      end

      unless ["table", "index"].include?(@type)
        @option_errors << "The type option for '#{@name}' must be 'table' or 'index', found: '#{@type}'"
        parsed = false
      end

      @columns_options = @options["columns"]
      unless @columns_options && @columns_options.is_a?(Array)
        @option_errors << "The options for '#{@name}' must include a 'columns' array"
        parsed = false
      end

      @columns = []
      sizes = @columns_options.map{|option| option.is_a?(Array) ? option.size : -1}.uniq
      if sizes == [2]
        @columns_options.each do |option|
          column = Column.new(option)
          if column.valid?
            @columns << column
          else
            @option_errors << column.formatted_errors
            parsed = false
          end
        end
      elsif sizes == [-1]
        @columns_options.each do |option|
          if option.is_a?(String)
            @columns << option.to_sym
          else
            @option_errors << "The column name for an 'index' type with name: '#{@name}' must be a string"
            parsed = false
          end
        end
      else
        @option_errors << "The columns array for '#{@name}' is not uniform, it should contain either arrays of two strings or only strings"
        parsed = false
      end

      @valid = parsed
    end
  end
end end end
