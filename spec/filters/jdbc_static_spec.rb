# encoding: utf-8
require_relative "spec_helper"

require "logstash/devutils/rspec/spec_helper"
require "logstash/filters/jdbc_static"
require 'jdbc/derby'
require "sequel"
require "sequel/adapters/jdbc"

module LogStash module Filters
  describe JdbcStatic do
    let(:db1) { ::Sequel.connect("jdbc:derby:memory:testdb;create=true", :user=> nil, :password=> nil) }
    let(:test_loader) { "SELECT * FROM reference_table" }
    let(:test_records) { db1[test_loader].all }

    let(:local_db_objects) do
      [
        {"type" => "table", "name" => "servers", "columns" => [["ip", "varchar(64)"], ["name", "varchar(64)"], ["location", "varchar(64)"]]},
        {"type" => "index", "name" => "servers_idx", "table" => "servers", "columns" => ["ip"]}
      ]
    end

    let(:settings) do
      {
        "loaders" => [
          {
            "id" =>"servers",
            "query" => "select ip, name, location from reference_table",
            "local_table" => "servers"
          }
        ],
        "local_db_objects" => local_db_objects,
        "local_lookups" => [
          {
            "query" => "select * from servers WHERE ip LIKE :ip",
            "parameters" => {"ip" => "%%{[ip]}"},
            "target" => "server"
          }
        ]
      }
    end

    let(:mixin_settings) do
      { "jdbc_user" => ENV['USER'], "jdbc_driver_class" => "org.apache.derby.jdbc.EmbeddedDriver",
        "jdbc_connection_string" => "jdbc:derby:memory:testdb;create=true" }
    end
    let(:plugin) { JdbcStatic.new(mixin_settings.merge(settings)) }

    before do
      db1.drop_table(:reference_table) rescue nil
      db1.create_table :reference_table do
        String :ip
        String :name
        String :location
      end
      db1[:reference_table].insert(:ip => "10.1.1.1", :name => "ldn-server-1", :location => "LDN-2-3-4")
      db1[:reference_table].insert(:ip => "10.2.1.1", :name => "nyc-server-1", :location => "NYC-5-2-8")
      db1[:reference_table].insert(:ip => "10.3.1.1", :name => "mv-server-1", :location => "MV-9-6-4")

      plugin.register
    end

    after { plugin.stop }

    let(:event)      { ::LogStash::Event.new("message" => "some text", "ip" => ipaddr) }

    let(:ipaddr) { ".3.1.1" }

    it "enhances an event" do
      plugin.filter(event)
      expect(event.get("server")).to eq([{"ip"=>"10.3.1.1", "name"=>"mv-server-1", "location"=>"MV-9-6-4"}])
    end
  end
end end
