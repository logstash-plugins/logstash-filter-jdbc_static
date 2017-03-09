module LogStash module Filters module Util
  class SingleLoadRunner

    attr_reader :local, :loaders, :preloaders, :postloaders

    def initialize(local, loaders, preloaders, postloaders)
      @local = local
      @loaders = loaders
      @preloaders = preloaders
      @postloaders = postloaders
    end

    def initial_load
      do_preload
      local.populate_all(loaders)
      do_postload
    end

    def repeated_load
    end

    def call
      repeated_load
    end

    # ----------------
    private

    def do_preload
      preloaders.each do |statement|
        local.run(statement)
      end
    end

    def do_postload
      postloaders.each do |statement|
        local.run(statement)
      end
    end
  end

end end end
