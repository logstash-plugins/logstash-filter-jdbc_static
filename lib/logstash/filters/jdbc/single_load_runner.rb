# encoding: utf-8
require_relative 'db_object'

module LogStash module Filters module Jdbc
  class SingleLoadRunner

    attr_reader :local, :loaders, :preloaders

    def initialize(local, loaders, preloaders)
      @local = local
      @loaders = loaders
      @preloaders = []
      @reload_counter = Concurrent::AtomicFixnum.new(0)
      @stopping = Concurrent::AtomicBoolean.new
      preloaders.map do |pre|
        @preloaders << DbObject.new(pre)
      end
      @preloaders.sort!
    end

    def initial_load
      do_preload
      local.populate_all(loaders)
      @reload_counter.increment
    end

    def repeated_load
    end

    def call
      # scheduler entry point
      return if @stopping.true?
      repeated_load
    end

    def reload_count
      @reload_counter.value
    end

    def stop
      @stopping.make_true
    end

    private

    def do_preload
      preloaders.each do |db_object|
        local.build_db_object(db_object)
      end
    end
  end

end end end
