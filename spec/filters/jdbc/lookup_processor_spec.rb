# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/filters/jdbc/lookup_processor"

module LogStash module Filters module Jdbc
  describe LookupProcessor do
    describe "class method find_validation_errors" do
      context "when supplied with an invalid arg" do
        it "nil as arg, fails validation" do
          result = described_class.find_validation_errors(nil)
          expect(result).to eq("The options must be an Array")
        end

        it "hash as arg, fails validation" do
          result = described_class.find_validation_errors({})
          expect(result).to eq("The options must be an Array")
        end

        it "array of lookup hash without query key as arg, fails validation" do
          lookup_hash = {
          "parameters" => {"ip" => "%%{[ip]}"},
          "target" => "server"
          }
          result = described_class.find_validation_errors([lookup_hash])
          expect(result).to eq("The options for 'lookup-1' must include a 'query' string")
        end

        it "array of lookup hash with bad parameters value as arg, fails validation" do
          lookup_hash = {
          "query" => "select * from servers WHERE ip LIKE :ip",
          "parameters" => %w(ip %%{[ip]}),
          "target" => "server"
          }
          result = described_class.find_validation_errors([lookup_hash])
          expect(result).to eq("The 'parameters' option for 'lookup-1' must be a Hash")
        end

        it "array of lookup hash with bad parameters value as arg and no target, fails validation" do
          lookup_hash = {
          "query" => "select * from servers WHERE ip LIKE :ip",
          "parameters" => %w(ip %%{[ip]})
          }
          result = described_class.find_validation_errors([lookup_hash])
          expect(result).to eq("The 'parameters' option for 'lookup-1' must be a Hash")
        end

        it "array of lookup hashes with invalid settings, fails validation with messages from each lookup" do
          lookup_hash1 = {
          "query" => "select * from servers WHERE ip LIKE :ip",
          "parameters" => ["ip", "%%{[ip]}"],
          "target" => "server"
          }
          lookup_hash2 = {
          "parameters" => {"id" => "%%{[id]}"},
          "default_hash" => {},
          "target" => "server"
          }
          result = described_class.find_validation_errors([lookup_hash1, lookup_hash2])
          expected = ["The 'parameters' option for 'lookup-1' must be a Hash"]
          expected << "The options for 'lookup-2' must include a 'query' string"
          expected << "Target fields must be different across all lookups, 'lookup-1', 'lookup-2' have the same target field defined"
          expect(result).to eq(expected.join("; "))
        end

        it "array of valid lookup hashes as arg with the same target, fails validation" do
          lookup_hash1 = {
          "id" => "L1",
          "query" => "select * from servers WHERE ip LIKE :ip",
          "parameters" => {"ip" => "%%{[ip]}"},
          "target" => "server"
          }
          lookup_hash2 = {
          "id" => "L2",
          "query" => "select * from users WHERE id LIKE :id",
          "parameters" => {"id" => "%%{[id]}"},
          "target" => "server"
          }
          lookup_hash3 = {
          "id" => "L3",
          "query" => "select * from table1 WHERE ip LIKE :ip",
          "parameters" => {"ip" => "%%{[ip]}"},
          "target" => "somefield"
          }
          lookup_hash4 = {
          "id" => "L4",
          "query" => "select * from table2 WHERE id LIKE :id",
          "parameters" => {"id" => "%%{[id]}"},
          "target" => "somefield"
          }
          result = described_class.find_validation_errors([lookup_hash1, lookup_hash2, lookup_hash3, lookup_hash4])
          expect(result).to eq("Target fields must be different across all lookups, 'L1', 'L2' have the same target field defined, 'L3', 'L4' have the same target field defined")
        end
      end

      context "when supplied with a valid arg" do
        it "empty array as arg, passes validation" do
          result = described_class.find_validation_errors([])
          expect(result).to eq(nil)
        end

        it "array of valid lookup hash as arg, passes validation" do
          lookup_hash = {
          "query" => "select * from servers WHERE ip LIKE :ip",
          "parameters" => {"ip" => "%%{[ip]}"},
          "target" => "server"
          }
          result = described_class.find_validation_errors([lookup_hash])
          expect(result).to eq(nil)
        end
      end
    end
  end
end end end

