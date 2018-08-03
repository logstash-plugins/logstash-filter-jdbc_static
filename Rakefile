# encoding: utf-8
require "jars/installer"
require "fileutils"
require "logstash/devutils/rake"

task :default do
  system('rake -vT')
end

task :vendor do
  exit(1) unless system './gradlew vendor'
end

task :clean do
  ["vendor/jar-dependencies", "Gemfile.lock"].each do |p|
    FileUtils.rm_rf(p)
  end
end

desc "Do bundle install and write gradle.properties"
task :bundle_install do
  `bundle install`
  delete_create_gradle_properties
end

desc "Write gradle.properties" # used by travis
task :write_gradle_properties do
  delete_create_gradle_properties
end

def delete_create_gradle_properties
  root_dir = File.dirname(__FILE__)
  gradle_properties_file = "#{root_dir}/gradle.properties"
  lsc_path = `bundle show logstash-core`.split(/\n/).first

  FileUtils.rm_f(gradle_properties_file)
  File.open(gradle_properties_file, "w") do |f|
    f.puts "logstashCoreGemPath=#{lsc_path}"
  end
  puts "-------------------> Wrote #{gradle_properties_file}"
  puts `cat #{gradle_properties_file}`
end