require_relative "basic_database"

module LogStash module Filters module Jdbc
  class ReadWriteDatabase < BasicDatabase

    def repopulate_all(loaders)
      case loaders.size
        when 1
          fill_and_switch(loaders.first)
        when 2
          fill_and_switch(loaders.first)
          fill_and_switch(loaders.last)
        else
          loaders.each do |loader|
            fill_and_switch(loader)
          end
      end
    end

    alias populate_all repopulate_all

    def fetch(statement, parameters)
      @rwlock.readLock().lock()
      @db[statement, parameters].all
    ensure
      @rwlock.readLock().unlock()
    end

    def build_db_object(db_object)
      @rwlock.writeLock().lock()
      db_object.build(@db)
    ensure
      @rwlock.writeLock().unlock()
    end

    private

    def fill_and_switch(loader)
      records = loader.fetch
      return if records.size.zero?

      @db[loader.temp_table].multi_insert(records)
      begin
        @rwlock.writeLock().lock()

        tmp = SecureRandom.hex(10)
        @db.rename_table(loader.temp_table, tmp)
        @db.rename_table(loader.table, loader.temp_table)
        @db.rename_table(tmp, loader.table)
        @db[loader.temp_table].truncate
      ensure
        @rwlock.writeLock().unlock()
      end
    end

    def post_initialize()
      # get a fair reentrant read write lock
      @rwlock = java.util.concurrent.locks.ReentrantReadWriteLock.new(true)
    end
  end
end end end
