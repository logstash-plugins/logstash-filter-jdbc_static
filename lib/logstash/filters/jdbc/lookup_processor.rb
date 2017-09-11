require_relative "lookup"
require_relative "read_write_database"

module LogStash module Filters module Jdbc
  class LookupProcessor
    attr_reader :lookups, :local

    def initialize(lookups_array, globals)
      @lookups = lookups_array.map.with_index do |options, i|
        Lookup.new(options, globals, "lookup-#{i.next}")
      end
      @local = ReadWriteDatabase.new()
      @local.connect(*globals.values_at(
        "lookup_jdbc_connection_string",
        "lookup_jdbc_driver_class",
        "lookup_jdbc_driver_library"))
    end

    def enhance(event)
      @lookups.each { |lookup| lookup.enhance(@local, event) }
    end
  end
end end end
