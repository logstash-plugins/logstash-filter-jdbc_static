require_relative "basic_database"

module LogStash module Filters module Util
  class ReadWriteDatabase < BasicDatabase
    def populate_all(loaders)
      loaders.each do |loader|
        populate(loader)
      end
    end

    def repopulate_all(loaders)
      loaders.each do |loader|
        repopulate(loader)
      end
    end

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

    # -----------------
    private

    def populate(loader)
      fill_and_switch(loader)
    end

    def repopulate(loader)
      fill_and_switch(loader)
    end

    def fill_and_switch(loader)
      @db[loader.temp_table].multi_insert(loader.fetch)
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
__END__

CREATE TABLE "REFERENCE_TABLE" ("IP" varchar(255), "NAME" varchar(255), "LOCATION" varchar(255))
INSERT INTO "REFERENCE_TABLE" ("IP", "NAME", "LOCATION") VALUES ('10.1.1.1', 'ldn-server-1', 'LDN-2-3-4')
INSERT INTO "REFERENCE_TABLE" ("IP", "NAME", "LOCATION") VALUES ('10.2.1.1', 'nyc-server-1', 'NYC-5-2-8')
INSERT INTO "REFERENCE_TABLE" ("IP", "NAME", "LOCATION") VALUES ('10.3.1.1', 'mv-server-1', 'MV-9-6-4')
CREATE TABLE "SERVERS" ("IP" varchar(64), "NAME" varchar(64), "LOCATION" varchar(64))
CREATE TABLE "TEMP_SERVERS" ("IP" varchar(64), "NAME" varchar(64), "LOCATION" varchar(64))
CREATE INDEX "SERVERS_IDX" ON "SERVERS" ("IP")
select ip, name, location from reference_table
INSERT INTO "TEMP_SERVERS" ("IP", "NAME", "LOCATION") VALUES ('10.1.1.1', 'ldn-server-1', 'LDN-2-3-4'), ('10.2.1.1', 'nyc-server-1', 'NYC-5-2-8'), ('10.3.1.1', 'mv-server-1', 'MV-9-6-4')
RENAME TABLE "TEMP_SERVERS" TO "81FF466CA53AE6516551"
RENAME TABLE "SERVERS" TO "TEMP_SERVERS"
RENAME TABLE "81FF466CA53AE6516551" TO "SERVERS"
TRUNCATE TABLE "TEMP_SERVERS"
select * from servers WHERE ip LIKE '%.3.1.1'
