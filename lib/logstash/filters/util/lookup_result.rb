module LogStash module Filters module Util
  class LookupResult
    attr_reader :payload

    def initialize
      @failure = false
      @payload = []
    end

    def push(data)
      @payload << data
    end

    def failed!
      @failure = true
    end

    def failed?
      @failure
    end

    def empty?
      @payload.empty?
    end
  end
end end end
