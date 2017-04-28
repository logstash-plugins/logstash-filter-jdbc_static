require_relative "lookup"
require_relative "read_write_database"

module LogStash module Filters module Util
  class LookupProcessor
    attr_reader :lookups, :local

    def initialize(lookups_array, globals, logger)
      @lookups = lookups_array.map.with_index do |options, i|
        Lookup.new(options, globals, logger, "lookup-#{i.next}")
      end
      @local = ReadWriteDatabase.new()
      @local.connect(*globals.values_at(
        "lookup_jdbc_connection_string",
        "lookup_jdbc_driver_class",
        "lookup_jdbc_driver_library"))
    end

    def enhance(event)
      @lookups.each do |lookup|
        result = lookup.enhance(@local, event)
      end
    end
  end
end end end

__END__
connect(connection_string, driver_class, driver_library, user, password)
options["lookup_jdbc_driver_class"] = @lookup_jdbc_driver_class
options["lookup_jdbc_connection_string"] = @lookup_jdbc_connection_string
