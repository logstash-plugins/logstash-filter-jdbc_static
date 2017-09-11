# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/filters/jdbc/db_object"

describe LogStash::Filters::Jdbc::DbObject  do
  context "various invalid non-hash arguments" do
    it "a nil does not validate" do
      instance = described_class.new(nil)
      expect(instance.valid?).to be_falsey
      expect(instance.formatted_errors).to eq("DbObject options must be a Hash")
    end

    it "a string does not validate" do
      instance = described_class.new("foo")
      expect(instance.valid?).to be_falsey
      expect(instance.formatted_errors).to eq("DbObject options must be a Hash")
    end

    it "a number does not validate" do
      instance = described_class.new(42)
      expect(instance.valid?).to be_falsey
      expect(instance.formatted_errors).to eq("DbObject options must be a Hash")
    end
  end

  context "various invalid hash arguments" do
    let(:error_messages) do
      [
        "DbObject options must include a 'name' string",
        "DbObject options for 'foo' must include a 'type' string",
        "The type option for 'foo' must be 'table' or 'index', found: ''",
        "DbObject options for 'foo' must include a 'columns' array",
        "The table option for 'foo' is missing, an index type needs a table to define an index on"
      ]
    end

    it "an empty hash does not validate" do
      instance = described_class.new({})
      expect(instance.valid?).to be_falsey
      expect(instance.formatted_errors).to eq(error_messages.values_at(0,1,2,3).join(", ").gsub('foo', 'unnamed'))
    end

    it "a name key value only" do
      instance = described_class.new({"name" => "foo"})
      expect(instance.valid?).to be_falsey
      expect(instance.formatted_errors).to eq(error_messages.values_at(1,2,3).join(", "))
    end

    it "a name and bad type" do
      instance = described_class.new({"name" => "foo", "type" => "indrex"})
      expect(instance.valid?).to be_falsey
      expect(instance.formatted_errors).to eq(error_messages.values_at(2,3).join(", ").gsub("''", "'indrex'"))
    end

    it "a name and good type only" do
      instance = described_class.new({"name" => "foo", "type" => "index"})
      expect(instance.valid?).to be_falsey
      expect(instance.formatted_errors).to eq(error_messages.values_at(4, 3).join(", "))
    end

    it "a name, good type and bad columns" do
      instance = described_class.new({"name" => "foo", "type" => "table", "columns" => 42})
      expect(instance.valid?).to be_falsey
      expect(instance.formatted_errors).to eq(error_messages[3])
    end

    it "a name, good type and bad columns - empty array" do
      instance = described_class.new({"name" => "foo", "type" => "table", "columns" => []})
      expect(instance.valid?).to be_falsey
      msg = "The columns array for 'foo' is not uniform, it should contain either arrays of two strings or only strings"
      expect(instance.formatted_errors).to eq(msg)
    end

    it "a name, good type and bad columns - irregular arrays" do
      instance = described_class.new({"name" => "foo", "type" => "table", "columns" => [["ip", "text"], ["name"], ["a", "b", "c"]]})
      expect(instance.valid?).to be_falsey
      msg = "The columns array for 'foo' is not uniform, it should contain either arrays of two strings or only strings"
      expect(instance.formatted_errors).to eq(msg)
    end

    it "a name, index type, no table and good columns" do
      instance = described_class.new({"name" => "foo_index", "type" => "index", "columns" => ["a", "b"]})
      expect(instance.valid?).to be_falsey
      msg = "The table option for 'foo_index' is missing, an index type needs a table to define an index on"
      expect(instance.formatted_errors).to eq(msg)
    end
  end

  context "a valid hash argument" do
    it "table type does validate" do
      instance = described_class.new({"name" => "foo", "type" => "table", "columns" => [["ip", "text"], ["name", "text"]]})
      expect(instance.valid?).to be_truthy
      expect(instance.formatted_errors).to eq("")
    end

    it "index type does validate" do
      instance = described_class.new({"name" => "foo", "table" => "servers", "type" => "index", "columns" => ["ip","name"]})
      expect(instance.valid?).to be_truthy
      expect(instance.formatted_errors).to eq("")
    end
  end
end