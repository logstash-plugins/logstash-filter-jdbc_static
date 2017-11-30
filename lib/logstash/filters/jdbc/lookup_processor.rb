require_relative "lookup"
require_relative "read_write_database"

module LogStash module Filters module Jdbc
  class LookupProcessor
    attr_reader :lookups, :local

    CONNECTION_ERROR_MSG = "Connection error when initialising lookup (local) db"
    DISCONNECTION_ERROR_MSG = "Connection error when disconnecting from lookup (local) db"

    def initialize(lookups_array, globals)
      @lookups = lookups_array.map.with_index do |options, i|
        Lookup.new(options, globals, "lookup-#{i.next}")
      end
      @local = ReadWriteDatabase.create(*globals.values_at(
        "lookup_jdbc_connection_string",
        "lookup_jdbc_driver_class",
        "lookup_jdbc_driver_library").compact)
      @local.connect(CONNECTION_ERROR_MSG)
    end

    def enhance(event)
      @lookups.each { |lookup| lookup.enhance(@local, event) }
    end

    def close
      @local.disconnect(DISCONNECTION_ERROR_MSG)
      @local = nil
    end
  end
end end end
