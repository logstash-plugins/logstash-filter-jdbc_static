require_relative "basic_database"

module LogStash module Filters module Util
  class ReadOnlyDatabase < BasicDatabase
    def query(statement)
      @db[statement].all
    end
  end
end end end
