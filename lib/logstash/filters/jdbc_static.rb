# encoding: utf-8
require "logstash/filters/base"
require "logstash/namespace"
require_relative "util/loader"
require_relative "util/repeating_load_runner"
require_relative "util/lookup_processor"

# This filter executes a SQL query and store the result set in the field
# specified as `target`.
# It will cache the results locally in an LRU cache with expiry
#
# For example you can load a row based on an id from in the event
#
# [source,ruby]
# filter {
#   jdbc_static {
#     jdbc_driver_library => "/path/to/mysql-connector-java-5.1.34-bin.jar"
#     jdbc_driver_class => "com.mysql.jdbc.Driver"
#     jdbc_connection_string => ""jdbc:mysql://localhost:3306/mydatabase"
#     jdbc_user => "me"
#     jdbc_password => "secret"
#     statement => "select * from WORLD.COUNTRY WHERE Code = :code"
#     parameters => { "code" => "country_code"}
#     target => "country_details"
#   }
# }
#
module LogStash module Filters class JdbcStatic < LogStash::Filters::Base
  config_name "jdbc_static"

  # Define the set (Hash) of loaders to fetch remote data and create local tables.
  # the fetched data will be inserted into the local tables. Make sure that the
  # local table name, columns and datatypes correspond to the shape of the remote data
  # being fetched.
  # You can also specify alternate JDBC strings to use if there is more than one remote database
  # For example:
  # loaders => {
  #   "country_details" => {
  #     "query" => "select code, name from WORLD.COUNTRY",
  #     "max_rows" => 2000
  #   },
  #   "servers" => {
  #     "jdbc_driver_class" => "org.postgresql.Driver",
  #     "jdbc_connection_string" => "jdbc:postgres://user:password@remotedb/infra"
  #     "query" => "select ip, name, location from INTERNAL.SERVERS",
  #   }
  # }
  config :loaders, :validate => :hash, :required => true

  # Define an array of SQL DDL statements to execute when the plugin first starts.
  # These will usually be the creational SQL to setup the local in-memory tables.
  # For example:
  # before_load_sql => [
  #   "CREATE TABLE country (code VARCHAR(3) NOT NULL, name VARCHAR(32) NOT NULL)",
  #   "CREATE TABLE servers (ip VARCHAR(16) NOT NULL, name VARCHAR(32) NOT NULL, location VARCHAR(32) NOT NULL)",
  #   "CREATE INDEX country_code_index ON country(code)",
  #   "CREATE INDEX servers_ip_index ON servers(ip)"
  # ]
  # NOTE: Important! Tables created here must have the same names as those used in the `loaders` and
  # `lookups` configuration options
  config :before_load_sql, :validate => :array, :default => []

  # Define an array of SQL DDL statements to execute after the remote data is fetched
  # and inserted locally. If the CRON schedule is specifed, the remote data is periodically
  # fetched and so these SQL statements will be executed repeatedly.
  config :after_load_sql, :validate => :array, :default => []

  # Define the set (Hash) of enhancement lookups to be applied to an event
  # The key is the target field and the value is a hash of the query string and an optional
  # parameter hash. Target is overwritten if existing.
  # Use parameters to have this plugin put values from the event into the query.
  # For example:
  # lookups => {
  #   "country_details" => {
  #     "query" => "select * from country WHERE code = :code",
  #     "parameters" => {"code" => "country_code"}
  #   },
  #   "servers" => {
  #     "query" => "select * from servers WHERE ip = :ip",
  #     "parameters" => {"ip" => "ip"}
  #   }
  # }
  config :lookups, :validate => :hash, :required => true

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

  # JDBC driver library path to third party driver library.
  config :jdbc_driver_library, :validate => :path

  # JDBC driver class to load, for example "oracle.jdbc.OracleDriver" or "org.apache.derby.jdbc.ClientDriver"
  config :jdbc_driver_class, :validate => :string, :required => true

  # JDBC connection string
  config :jdbc_connection_string, :validate => :string, :required => true

  # JDBC user
  config :jdbc_user, :validate => :string

  # JDBC password
  config :jdbc_password, :validate => :password

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
  private


  def prepare_runner
    @parsed_loaders = @loaders.map do |table, options|
      add_plugin_configs(options)
      loader = Util::Loader.new(table, options)
      loader.build_remote_db
      loader
    end

    @processor = Util::LookupProcessor.new(@lookups, global_lookup_options(), @logger)

    if @schedule
      require "rufus/scheduler"
      @loader_runner = Util::RepeatingLoadRunner.new(
          @processor.local, @parsed_loaders, @before_load_sql, @after_load_sql)

      @scheduler = Rufus::Scheduler.new(:max_work_threads => 1)
      @scheduler.cron(@schedule, @loader_runner)
      @scheduler.join
    else
      @loader_runner = Util::SingleLoadRunner.new(
          @processor.local, @parsed_loaders, @before_load_sql, @after_load_sql)
    end
  end

  def global_lookup_options(options = Hash.new)
    if @tag_on_failure && !@tag_on_failure.empty? && !options.key?("tag_on_failure")
      options["tag_on_failure"] = @tag_on_failure
    end
    if @tag_on_default_use && !@tag_on_default_use.empty? && !options.key?("tag_on_default_use")
      options["tag_on_default_use"] = @tag_on_default_use
    end
    options
  end

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
end end end
