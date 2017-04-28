require_relative "basic_database"

module LogStash module Filters module Util
  class ReadOnlyDatabase < BasicDatabase
    def count(statement)
      @db[statement].count
    end

    def query(statement)
      @db[statement].all
    end
  end
end end end
