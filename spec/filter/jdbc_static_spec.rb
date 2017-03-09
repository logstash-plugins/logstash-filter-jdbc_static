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

    let(:preloaders) do
      [
        "CREATE TABLE servers (ip VARCHAR(16) NOT NULL, name VARCHAR(32) NOT NULL, location VARCHAR(32) NOT NULL)",
        "CREATE INDEX servers_ip_index ON servers(ip)"
      ]
    end

    let(:lookup) { "SELECT name, location FROM servers WHERE ip = ?" }

    let(:settings) do
      {
        "loaders" => {
          "servers" => {
            "query" => "select ip, name, location from reference_table"
          }
        },
        "before_load_sql" => preloaders,
        "lookups" => {
          "server" => {
            "query" => "select * from servers WHERE ip = :ip",
            "parameters" => {"ip" => "ip"}
          }
        }
      }
    end
    let(:mixin_settings) do
      { "jdbc_user" => ENV['USER'], "jdbc_driver_class" => "org.apache.derby.jdbc.EmbeddedDriver",
        "jdbc_connection_string" => "jdbc:derby:memory:testdb;create=true" }
    end
    let(:plugin) { JdbcStatic.new(mixin_settings.merge(settings)) }

    before do
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

    let(:event)      { ::LogStash::Event.new("message" => "some text", "ip" => ipaddr) }

    let(:ipaddr) { "10.3.1.1" }

    it "enhances an event" do
      plugin.filter(event)
      expect(event.get("server")).to eq([{"ip"=>"10.3.1.1", "name"=>"mv-server-1", "location"=>"MV-9-6-4"}])
    end
  end
end end
