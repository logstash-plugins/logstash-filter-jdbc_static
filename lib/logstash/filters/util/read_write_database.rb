require_relative "basic_database"

module LogStash module Filters module Util
  class ReadWriteDatabase < BasicDatabase
    def populate_all(loaders)
      @mutex.synchronize do
        loaders.each do |loader|
          populate(loader)
        end
      end
    end

    def repopulate_all(loaders)
      @mutex.synchronize do
        loaders.each do |loader|
          repopulate(loader)
        end
      end
    end

    def run(statement)
      @mutex.synchronize do
        @db.run(statement)
      end
    end

    def fetch(statement, parameters)
      @mutex.synchronize do
        @db[statement, parameters].all
      end
    end

    # -----------------
    private

    def populate(loader)
      @db[loader.table].multi_insert(loader.fetch)
    end

    def repopulate(loader)
      @db[loader.table].truncate
      records = remote.query(loader)
      @db[loader.table].multi_insert(loader.fetch)
    end

    def pre_connect(connection_string, driver_class, driver_library, user, password)
      require 'jdbc/derby'
      Jdbc::Derby.load_driver
      Sequel::JDBC.load_driver("org.apache.derby.jdbc.EmbeddedDriver")
      @db = Sequel.connect("jdbc:derby:memory:localdb;create=true")
    end

    def post_initialize()
      @mutex = Mutex.new
    end
  end
end end end
