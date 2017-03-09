require_relative "lookup"
require_relative "read_write_database"

module LogStash module Filters module Util
  class LookupProcessor
    attr_reader :lookups, :local

    def initialize(lookups_hash, globals, logger)
      @lookups = lookups_hash.map do |target, options|
        Lookup.new(target, options, globals, logger)
      end
      @local = ReadWriteDatabase.new()
      @local.connect()
    end

    def enhance(event)
      @lookups.each do |lookup|
        result = lookup.enhance(@local, event)
      end
    end
  end
end end end
