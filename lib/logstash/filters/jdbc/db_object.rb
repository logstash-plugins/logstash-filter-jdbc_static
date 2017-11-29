require_relative "validatable"
require_relative "column"

module LogStash module Filters module Jdbc

  TEMP_TABLE_PREFIX = "temp_".freeze

  class DbObject < Validatable
    #   {name => "servers", index_columns => ["ip"], columns => [["ip", "text"], ["name", "text"], ["location", "text"]]},

    attr_reader :name, :columns, :preserve_existing, :index_columns

    def build(db)
      return unless valid?
      schema_gen = db.create_table_generator()
      @columns.each {|col| schema_gen.column(col.name, col.datatype)}
      schema_gen.index(@index_columns)
      options = {:generator => schema_gen}
      if @preserve_existing
        db.create_table?(@name, options)
      else
        db.create_table(@name, options)
      end
    end

    def <=>(other)
      @name <=> other.name
    end

    def as_temp_table_opts
      {"name" => "#{TEMP_TABLE_PREFIX}#{@name}", "preserve_existing" => @preserve_existing, "index_columns" => @index_columns.map(&:to_s), "columns" => @columns.map(&:to_array)}
    end

    def to_s
      inspect
    end

    def inspect
      "<LogStash::Filters::Jdbc::DbObject name: #{@name}, columns: #{@columns.inspect}>"
    end

    private

    def post_initialize
      if valid?
        @name = @name.to_sym
      end
    end

    def parse_options
      if !@options.is_a?(Hash)
        @option_errors << "DbObject options must be a Hash"
        @valid = false
        return
      end

      parsed = true
      @name = @options["name"]
      unless @name && @name.is_a?(String)
        @option_errors << "DbObject options must include a 'name' string"
        @name = "unnamed"
        parsed = false
      end

      @preserve_existing = @options.fetch("preserve_existing", false)
      @preserve_existing = true if @preserve_existing == "true"

      @columns_options = @options["columns"]
      @columns = []
      temp_column_names = []
      if @columns_options && @columns_options.is_a?(Array)
        sizes = @columns_options.map{|option| option.size}.uniq
        if sizes == [2]
          @columns_options.each do |option|
            column = Column.new(option)
            if column.valid?
              @columns << column
              temp_column_names << column.name
            else
              @option_errors << column.formatted_errors
              parsed = false
            end
          end
        else
          @option_errors << "The columns array for '#{@name}' is not uniform, it should contain arrays of two strings only"
          parsed = false
        end
      else
        @option_errors << "DbObject options for '#{@name}' must include a 'columns' array"
        parsed = false
      end

      @index_column_options = @options["index_columns"]
      @index_columns = []
      if @index_column_options && @index_column_options.is_a?(Array)
        @index_column_options.each do |option|
          if option.is_a?(String) && temp_column_names.member?(option.to_sym)
            @index_columns << option.to_sym
          else
            @option_errors << "The index_columns element: '#{option}' must be a column defined in the columns array"
            parsed = false
          end
        end
      end

      @valid = parsed
    end
  end
end end end
