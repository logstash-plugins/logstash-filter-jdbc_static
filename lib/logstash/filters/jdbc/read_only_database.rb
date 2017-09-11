# encoding: utf-8
require_relative "basic_database"

module LogStash module Filters module Jdbc
  class ReadOnlyDatabase < BasicDatabase
    def count(statement)
      @db[statement].count
    end

    def query(statement)
      @db[statement].all
    end

    def post_initialize()
    end
  end
end end end
