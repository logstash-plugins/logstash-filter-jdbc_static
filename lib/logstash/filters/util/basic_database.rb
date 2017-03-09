require "sequel"
require "sequel/adapters/jdbc"
require "java"

module LogStash module Filters module Util
  class BasicDatabase
    def initialize()
      post_initialize
    end

    def connect(
          connection_string = "jdbc:derby:memory:lookupdb;create=true",
          driver_class = "org.apache.derby.jdbc.EmbeddedDriver",
          driver_library = nil,
          user = nil,
          password = nil)
      pre_connect(connection_string, driver_class, driver_library, user, password)
      @db.test_connection
    end

    # -----------------
    private

    def pre_connect(connection_string, driver_class, driver_library, user, password)
      require driver_library if driver_library
      Sequel::JDBC.load_driver(driver_class)
      @db = Sequel.connect(connection_string, :user => user, :password =>  password.nil? ? nil : password.value)
    end

    def post_initialize()
      # overwrite in subclass for specific initialize code
    end
  end
end end end
