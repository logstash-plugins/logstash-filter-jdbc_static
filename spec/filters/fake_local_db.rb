# encoding: utf-8

module LogStash module Filters module Jdbc
  class FakeLocalDb
    def initialize(target, records)
      @target = target
      @records = records
    end

    def fetch_with_lock(lookup_id, event)
      event.set(@target, @records)
      LookupFailures.new
    end
  end
end end end
