require "sequel"
require "sequel/adapters/jdbc"
require "java"
require "logstash/util/loggable"

module LogStash module Filters module Jdbc
  class BasicDatabase
    include LogStash::Util::Loggable

    def initialize()
      post_initialize
    end

    def connect(
          connection_string = "jdbc:derby:memory:localdb;create=true",
          driver_class = "org.apache.derby.jdbc.EmbeddedDriver",
          driver_library = nil,
          user = nil,
          password = nil)
      pre_connect(connection_string, driver_class, driver_library, user, password)
      @db.test_connection
    end

    def disconnect
      @db.disconnect
    end

    def empty_record_set
      []
    end

    private

    def pre_connect(connection_string, driver_class, driver_library, user, password)
      require driver_library if driver_library
      Sequel::JDBC.load_driver(driver_class)
      if user && password
        @db = Sequel.connect(connection_string, :user => user, :password =>  password.value)
      elsif user
        @db = Sequel.connect(connection_string, :user => user)
      else
        @db = Sequel.connect(connection_string)
      end
    end

    def post_initialize()
      raise NotImplementedError.new("#{self.class.name} is abstract, you must subclass it and implement #post_initialize()")
    end
  end
end end end
