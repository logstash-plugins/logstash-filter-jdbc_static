require_relative "single_load_runner"

module LogStash module Filters module Util
  class RepeatingLoadRunner < SingleLoadRunner
   # info - attr_reader :local, :loaders, :preloaders, :postloaders

    def repeated_load
      local.repopulate_all(loaders)
      do_postload
    end
  end
end end end
