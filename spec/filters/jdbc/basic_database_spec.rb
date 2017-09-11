# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/filters/jdbc/basic_database"

describe LogStash::Filters::Jdbc::BasicDatabase do
  it "is abstract, creating an instance fails" do
    expect{described_class.new}.to raise_error(NotImplementedError)
  end

end
