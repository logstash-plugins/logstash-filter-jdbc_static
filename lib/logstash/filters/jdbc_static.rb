# encoding: utf-8
require "logstash/filters/base"
require "logstash/namespace"
require_relative "util/loader"
require_relative "util/db_object"
require_relative "util/repeating_load_runner"
require_relative "util/lookup_processor"

# This filter can do multiple enhancements to an event in one pass.
# Define multiple loader sources and multiple lookup targets.
#
# [source,ruby]

#
module LogStash module Filters class JdbcStatic < LogStash::Filters::Base
  config_name "jdbc_static"

  # Define the set (Hash) of loaders to fetch remote data and create local tables.
  # the fetched data will be inserted into the local tables. Make sure that the
  # local table name, columns and datatypes correspond to the shape of the remote data
  # being fetched.
  # You can also specify alternate JDBC strings to use if there is more than one remote database
  # For example:
  # loaders => [
  #   {
  #     "id" => "country_details"
  #     "query" => "select code, name from WORLD.COUNTRY"
  #     "max_rows" => 2000
  #     local_table => "country"
  #   },
  #   {
  #     id => "servers_load"
  #     query => "select id, ip, name, location from INTERNAL.SERVERS"
  #     local_table => "servers"
  #   }
  # ]
  # This is optional. You can provide a pre-populated local database server then no initial loaders are needed.
  config :loaders, :required => true, :default => [], :validate => [LogStash::Filters::Util::Loader]

  # Define an array of Database Objects to create when the plugin first starts.
  # These will usually be the definitions to setup the local in-memory tables.
  # For example:
  # local_db_objects => [
  #   {type => "table", name => "servers", columns => [["id", "INTEGER"], ["ip", "varchar(64)"], ["name", "varchar(64)"], ["location", "varchar(64)"]]},
  #   {type => "index", name => "servers_idx", table => "servers", columns => ["ip"]}
  # ]
  # NOTE: Important! Tables created here must have the same names as those used in the `loaders` and
  # `lookups` configuration options
  config :local_db_objects, :required => false, :default => [], :validate => [LogStash::Filters::Util::DbObject]

  # Define the list (Array) of enhancement lookups to be applied to an event
  # Each entry is a hash of the query string, the target field and value and a
  # parameter hash. Target is overwritten if existing. Target is optional,
  # if omitted the lookup results will be written to the root of the event like this:
  # event.set(<column name (or alias)>, <column value>)
  # Use parameters to have this plugin put values from the event into the query.
  # The parameter maps the symbol used in the query string to the field name in the event.
  # NOTE: when using a query string that includes the LIKE keyword make sure that
  # you provide a Logstash Event sprintf pattern with added wildcards.
  # For example:
  # lookups => [
  #   {
  #     "query" => "select * from country WHERE code = :code",
  #     "parameters" => {"code" => "country_code"}
  #     "target" => "country_details"
  #   },
  #   {
  #     "query" => "select ip, name from servers WHERE ip LIKE :ip",
  #     "parameters" => {"ip" => "%{[response][ip]}%"}
  #     "target" => "servers"
  #   }
  # ]
  config :lookups, :required => true, :validate => [LogStash::Filters::Util::Lookup]

  # Schedule of when to periodically run loaders, in Cron format
  # for example: "* * * * *" (execute query every minute, on the minute)
  #
  # There is no schedule by default. If no schedule is given, then the loaders are run
  # exactly once.
  config :schedule, :validate => :string

  # Append values to the `tags` field if sql error occured
  config :tag_on_failure, :validate => :array, :default => ["_jdbcstreamingfailure"]

  # Append values to the `tags` field if no record was found and default values were used
  config :tag_on_default_use, :validate => :array, :default => ["_jdbcstreamingdefaultsused"]

  # Remote Load DB JDBC driver library path to third party driver library.
  config :jdbc_driver_library, :validate => :path

  # Remote Load DB JDBC driver class to load, for example "oracle.jdbc.OracleDriver" or "org.apache.derby.jdbc.ClientDriver"
  config :jdbc_driver_class, :validate => :string, :required => true

  # Remote Load DB JDBC connection string
  config :jdbc_connection_string, :validate => :string, :required => true

  # Remote Load DB JDBC user
  config :jdbc_user, :validate => :string

  # Remote Load DB JDBC password
  config :jdbc_password, :validate => :password

  # Local Lookup DB JDBC driver class to load, for example "org.apache.derby.jdbc.ClientDriver"
  config :lookup_jdbc_driver_class, :validate => :string, :required => false, :default => "org.apache.derby.jdbc.EmbeddedDriver"

  # Local Lookup DB JDBC driver library path to third party driver library.
  config :lookup_jdbc_driver_library, :validate => :path, :required => false

  # Local Lookup DB JDBC connection string
  config :lookup_jdbc_connection_string, :validate => :string, :required => false, :default => "jdbc:derby:memory:localdb;create=true"

  class << self
    alias_method :old_validate_value, :validate_value

    def validate_value(value, validator)

      result = value
      if validator.is_a?(Array) && validator.first.is_a?(Class)
        validation_error = validator.first.validate(value)
        unless validation_error.nil?
          return false, validation_error
        end
      else
        return old_validate_value(value, validator)
      end
      [true, result]
    end
  end

  public

  def register
    # @symbol_parameters = @parameters.inject({}) {|hash,(k,v)| hash[k.to_sym] = v ; hash }
    prepare_runner
    @loader_runner.initial_load
  end

  def filter(event)
    @processor.enhance(event)
    filter_matched(event)
  end

  # ---------------------
  # temp while developing
  def local
    @processor.local
  end
  # ---------------------


  def stop
    @scheduler.stop if @scheduler
    @parsed_loaders.each(&:close)
    @loader_runner.close
  end

  private

  def prepare_runner
    @parsed_loaders = @loaders.map do |options|
      add_plugin_configs(options)
      loader = Util::Loader.new(options)
      loader.build_remote_db
      loader
    end

    @processor = Util::LookupProcessor.new(@lookups, global_lookup_options(), @logger)

    if @schedule
      require "rufus/scheduler"
      @loader_runner = Util::RepeatingLoadRunner.new(
          @processor.local, @parsed_loaders, @local_db_objects)

      @scheduler = Rufus::Scheduler.new(:max_work_threads => 1)
      @scheduler.cron(@schedule, @loader_runner)
      @scheduler.join
    else
      @loader_runner = Util::SingleLoadRunner.new(
          @processor.local, @parsed_loaders, @local_db_objects)
    end
  end

  def global_lookup_options(options = Hash.new)
    if @tag_on_failure && !@tag_on_failure.empty? && !options.key?("tag_on_failure")
      options["tag_on_failure"] = @tag_on_failure
    end
    if @tag_on_default_use && !@tag_on_default_use.empty? && !options.key?("tag_on_default_use")
      options["tag_on_default_use"] = @tag_on_default_use
    end
    options["lookup_jdbc_driver_class"] = @lookup_jdbc_driver_class
    options["lookup_jdbc_driver_library"] = @lookup_jdbc_driver_library
    options["lookup_jdbc_connection_string"] = @lookup_jdbc_connection_string
    options
  end

  def add_plugin_configs(options)
    if @jdbc_driver_library
      options["jdbc_driver_library"] = @jdbc_driver_library
    end
    if @jdbc_driver_class
      options["jdbc_driver_class"] = @jdbc_driver_class
    end
    if @jdbc_connection_string
      options["jdbc_connection_string"] = @jdbc_connection_string
    end
    if @jdbc_user
      options["jdbc_user"] = @jdbc_user
    end
    if @jdbc_password
      options["jdbc_password"] = @jdbc_password
    end
  end
end end end
__END__

  def add_plugin_configs(options)
    if @jdbc_driver_library && !options.key?("jdbc_driver_library")
      options["jdbc_driver_library"] = @jdbc_driver_library
    end
    if @jdbc_driver_class && !options.key?("jdbc_driver_class")
      options["jdbc_driver_class"] = @jdbc_driver_class
    end
    if @jdbc_connection_string && !options.key?("jdbc_connection_string")
      options["jdbc_connection_string"] = @jdbc_connection_string
    end
    if @jdbc_user && !options.key?("jdbc_user")
      options["jdbc_user"] = @jdbc_user
    end
    if @jdbc_password && !options.key?("jdbc_password")
      options["jdbc_password"] = @jdbc_password
    end
  end


VARCHAR java.lang.String  setString updateString
CHAR  java.lang.String  setString updateString
LONGVARCHAR java.lang.String  setString updateString
BIT boolean setBoolean  updateBoolean
NUMERIC java.math.BigDecimal  setBigDecimal updateBigDecimal
TINYINT byte  setByte updateByte
SMALLINT  short setShort  updateShort
INTEGER int setInt  updateInt
BIGINT  long  setLong updateLong
REAL  float setFloat  updateFloat
FLOAT float setFloat  updateFloat
DOUBLE  double  setDouble updateDouble
VARBINARY byte[ ] setBytes  updateBytes
BINARY  byte[ ] setBytes  updateBytes
DATE  java.sql.Date setDate updateDate
TIME  java.sql.Time setTime updateTime
TIMESTAMP java.sql.Timestamp  setTimestamp  updateTimestamp
CLOB  java.sql.Clob setClob updateClob
BLOB  java.sql.Blob setBlob updateBlob
ARRAY java.sql.Array  setARRAY  updateARRAY
REF java.sql.Ref  SetRef  updateRef
STRUCT  java.sql.Struct SetStruct updateStruct
