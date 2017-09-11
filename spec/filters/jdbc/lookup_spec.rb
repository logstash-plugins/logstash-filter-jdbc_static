# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/filters/jdbc/lookup"

module LogStash module Filters module Jdbc
  describe Lookup do
    describe "class method validate" do
      context "when supplied with an invalid arg" do
        it "nil as arg, fails validation" do
          result = described_class.validate(nil)
          expect(result).to eq("The options must be an Array")
        end

        it "hash as arg, fails validation" do
          result = described_class.validate({})
          expect(result).to eq("The options must be an Array")
        end

        it "array of lookup hash without query key as arg, fails validation" do
          lookup_hash = {
          "parameters" => {"ip" => "%%{[ip]}"},
          "target" => "server"
          }
          result = described_class.validate([lookup_hash])
          expect(result).to eq("The options for 'server' must include a 'query' string")
        end

        it "array of lookup hash with bad parameters value as arg, fails validation" do
          lookup_hash = {
          "query" => "select * from servers WHERE ip LIKE :ip",
          "parameters" => %w(ip %%{[ip]}),
          "target" => "server"
          }
          result = described_class.validate([lookup_hash])
          expect(result).to eq("The 'parameters' option for 'server' must be a Hash")
        end

        it "array of lookup hash with bad parameters value as arg and no target, fails validation" do
          lookup_hash = {
          "query" => "select * from servers WHERE ip LIKE :ip",
          "parameters" => %w(ip %%{[ip]})
          }
          result = described_class.validate([lookup_hash])
          expect(result).to eq("The 'parameters' option for 'lookup-1' must be a Hash")
        end
      end

      context "when supplied with a valid arg" do
        it "empty array as arg, passes validation" do
          result = described_class.validate([])
          expect(result).to eq(nil)
        end

        it "array of valid lookup hash as arg, passes validation" do
          lookup_hash = {
            "query" => "select * from servers WHERE ip LIKE :ip",
            "parameters" => {"ip" => "%%{[ip]}"},
            "target" => "server"
          }
          result = described_class.validate([lookup_hash])
          expect(result).to eq(nil)
        end
      end
    end

    describe "normal operations" do
      let(:local_db) { double("local_db") }
      let(:lookup_hash) do
        {
          "query" => "select * from servers WHERE ip LIKE :ip",
          "parameters" => {"ip" => "%%{[ip]}"},
          "target" => "server"
        }
      end
      let(:event) { LogStash::Event.new()}
      let(:records) { [{"name" => "ldn-1-23", "rack" => "2:1:6"}] }

      subject(:lookup) { described_class.new(lookup_hash, {}, "lookup-1") }

      before(:each) do
        allow(local_db).to receive(:fetch).once.and_return(records)
      end

      it "should be valid" do
        expect(subject.valid?).to be_truthy
      end

      it "should have no formatted_errors" do
        expect(subject.formatted_errors).to eq("")
      end

      it "should enhance an event" do
        subject.enhance(local_db, event)
        expect(event.get("server")).to eq(records)
      end
    end
  end
end end end

