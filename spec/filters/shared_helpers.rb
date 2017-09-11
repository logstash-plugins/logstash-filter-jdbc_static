# encoding: utf-8
require "logstash/filters/jdbc/db_object"

RSpec.shared_examples "a single load runner" do

  context "with local db objects" do
    let(:local_db_objects) do
      [
      {"type" => "table", "name" => "servers", "columns" => [%w(ip text), %w(name text), %w(location text)]},
      {"type" => "index", "name" => "servers_idx", "table" => "servers", "columns" => ["ip"]}
      ]
    end

    it "builds local db objects and populates the local db" do
      expect(local_db).to receive(:populate_all).once.with(loaders)
      expect(local_db).to receive(:build_db_object).thrice.with(instance_of(LogStash::Filters::Jdbc::DbObject))
      runner.initial_load
      expect(runner.preloaders).to be_a(Array)
      expect(runner.preloaders.size).to eq(3)
      expect(runner.preloaders[0].table?).to be_truthy
      expect(runner.preloaders[0].name).to eq(:servers)
      expect(runner.preloaders[1].table?).to be_truthy
      expect(runner.preloaders[1].name).to eq(:temp_servers)
      expect(runner.preloaders[2].index?).to be_truthy
      expect(runner.preloaders[2].name).to eq(:servers_idx)
      expect(runner.local).to eq(local_db)
      expect(runner.loaders).to eq(loaders)
    end
  end

  context "without local db objects" do
    it "populates the local db" do
      expect(local_db).to receive(:populate_all).once.with(loaders)
      runner.initial_load
      expect(runner.preloaders).to eq([])
      expect(runner.local).to eq(local_db)
      expect(runner.loaders).to eq(loaders)
    end
  end

  context "when shutting down" do
    let(:local_db_objects) { [] }

    it "disconnects from the local db" do
      expect(local_db).to receive(:populate_all).once.with(loaders)
      expect(local_db).to receive(:disconnect).once
      runner.initial_load
      runner.close
      expect(runner.preloaders).to eq([])
    end
  end
end