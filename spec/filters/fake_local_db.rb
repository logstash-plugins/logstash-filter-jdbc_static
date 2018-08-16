# encoding: utf-8

module LogStash module Filters module Jdbc
  class FakeLocalDb
    def initialize(target, records)
      @target = target
      @records = records
    end

    def fetch_with_lock(lookup_id, event, failures)
      event.set(@target, @records)
    end
  end
end end end
