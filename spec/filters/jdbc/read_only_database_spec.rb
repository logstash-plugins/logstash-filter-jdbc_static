# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/util/password"
require "logstash/filters/jdbc/read_only_database"

module LogStash module Filters module Jdbc
  describe ReadOnlyDatabase do
    let(:db) { double("Sequel::DB") }
    let(:connection_string) { "jdbc:derby:memory:localdb;create=true" }
    subject(:read_only_db) { described_class.new }

    before(:each) do
      expect(db).to receive(:test_connection).once
    end

    describe "basic operations" do
      context "connecting  to a db" do
        it "connects with defaults" do
          expect(Sequel::JDBC).to receive(:load_driver).once.with("org.apache.derby.jdbc.EmbeddedDriver")
          expect(Sequel).to receive(:connect).once.with(connection_string).and_return(db)
          read_only_db.connect
          expect(read_only_db.empty_record_set).to eq([])
        end

        it "connects with fully specified arguments" do
          connection_str = "a connection string"
          user = "a user"
          password = Util::Password.new("secret")
          expect(Sequel::JDBC).to receive(:load_driver).once.with("a driver class")
          expect(Sequel).to receive(:connect).once.with(connection_str, {:user => user, :password =>  password.value}).and_return(db)
          read_only_db.connect(connection_str, "a driver class", nil, user, password)
        end
      end

      describe "methods" do
        let(:dataset) { double("Sequel::Dataset") }
        before(:each) do
          allow(Sequel::JDBC).to receive(:load_driver)
          allow(Sequel).to receive(:connect).and_return(db)
          allow(db).to receive(:[]).and_return(dataset)
        end

        it "the count method gets a count from the dataset" do
          expect(dataset).to receive(:count)
          read_only_db.connect
          read_only_db.count("select * from table")
        end

        it "the query method gets all records from the dataset" do
          expect(dataset).to receive(:all)
          read_only_db.connect
          read_only_db.query("select * from table")
        end
      end
    end
  end
end end end
