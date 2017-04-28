require_relative "single_load_runner"

module LogStash module Filters module Util
  class RepeatingLoadRunner < SingleLoadRunner
   # info - attr_reader :local, :loaders, :preloaders

    def repeated_load
      local.repopulate_all(loaders)
    end
  end
end end end
