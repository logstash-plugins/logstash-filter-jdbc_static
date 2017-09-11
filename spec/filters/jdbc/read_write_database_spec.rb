# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/util/password"
require "logstash/filters/jdbc/db_object"
require "logstash/filters/jdbc/read_write_database"

module LogStash module Filters module Jdbc
  describe ReadWriteDatabase do
    let(:db) { double("Sequel::DB") }
    let(:connection_string) { "jdbc:derby:memory:localdb;create=true" }
    subject(:read_write_db) { described_class.new }

    before(:each) do
      expect(db).to receive(:test_connection).once
    end

    describe "basic operations" do
      context "connecting  to a db" do
        it "connects with defaults" do
          expect(Sequel::JDBC).to receive(:load_driver).once.with("org.apache.derby.jdbc.EmbeddedDriver")
          expect(Sequel).to receive(:connect).once.with(connection_string).and_return(db)
          read_write_db.connect
          expect(read_write_db.empty_record_set).to eq([])
        end

        it "connects with fully specified arguments" do
          connection_str = "a connection string"
          user = "a user"
          password = Util::Password.new("secret")
          expect(Sequel::JDBC).to receive(:load_driver).once.with("a driver class")
          expect(Sequel).to receive(:connect).once.with(connection_str, {:user => user, :password =>  password.value}).and_return(db)
          read_write_db.connect(connection_str, "a driver class", nil, user, password)
        end
      end

      describe "methods" do
        let(:dataset) { double("Sequel::Dataset") }
        let(:loaders) { [] }
        let(:loader)  { double("Loader") }
        let(:table_name) { "users" }
        let(:temp_name)  { "users_temp" }
        let(:random_table_name)  { "foobarbaz" }

        before(:each) do
          allow(Sequel::JDBC).to receive(:load_driver)
          allow(Sequel).to receive(:connect).and_return(db)
          allow(loader).to receive(:fetch).and_return([1,2,3])
          allow(loader).to receive(:table).and_return(table_name)
          allow(loader).to receive(:temp_table).and_return(temp_name)
          allow(SecureRandom).to receive(:hex).and_return(random_table_name)
          loaders.push(loader)
          loaders.push(loader)
        end

        it "the populate_all method fills a local_db from the dataset" do
          expect(db).to receive(:[]).with(loader.temp_table).exactly(4).and_return(dataset)
          expect(dataset).to receive(:multi_insert).twice.with([1,2,3])
          expect(db).to receive(:rename_table).twice.with(temp_name, random_table_name)
          expect(db).to receive(:rename_table).twice.with(table_name, temp_name)
          expect(db).to receive(:rename_table).twice.with(random_table_name, table_name)
          expect(dataset).to receive(:truncate).twice

          read_write_db.connect
          read_write_db.populate_all(loaders)
        end

        it "the repopulate_all method fills a local_db from the dataset" do
          expect(db).to receive(:[]).with(loader.temp_table).exactly(4).and_return(dataset)
          expect(dataset).to receive(:multi_insert).twice.with([1,2,3])
          expect(db).to receive(:rename_table).twice.with(temp_name, random_table_name)
          expect(db).to receive(:rename_table).twice.with(table_name, temp_name)
          expect(db).to receive(:rename_table).twice.with(random_table_name, table_name)
          expect(dataset).to receive(:truncate).twice

          read_write_db.connect
          read_write_db.repopulate_all(loaders)
        end

        it "the fetch method executes a parameterised SQL statement on the local db" do
          statement = "select 1 from dual"
          parameters = 42
          expect(db).to receive(:[]).with(statement, parameters).once.and_return(dataset)
          expect(dataset).to receive(:all).once.and_return([1,2,3])

          read_write_db.connect
          read_write_db.fetch(statement, parameters)
        end

        it "lends the local db to a DbObject build instance method" do
          db_object = DbObject.new("type" => "index", "name" => "servers_idx", "table" => "servers", "columns" => ["ip"])
          expect(db_object).to receive(:build).once.with(db)
          read_write_db.connect
          read_write_db.build_db_object(db_object)
        end
      end
    end
  end
end end end
