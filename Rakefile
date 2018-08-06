# encoding: utf-8
require "jars/installer"
require "fileutils"
require "logstash/devutils/rake"
require "rspec/core/rake_task"

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

desc "Compile and vendor java into ruby for travis, its done bundle install already"
task :travis_vendor => [:write_gradle_properties] do
  exit(1) unless system './gradlew check vendor'
  puts "-------------------> vendored jdbc_static jar via rake"
end

RSpec::Core::RakeTask.new(:spec)

task :check => [:vendor, :spec]

task :travis_test => [:travis_vendor, :spec]

desc "Run full check with custom Logstash path" # e.g. rake custom_ls_check[/elastic/tmp/logstash-5.5.3]
task(:custom_ls_check, [:ls_dir] => [:clean]) do |task, args|
  ls_path = args[:ls_dir]
  system(custom_ls_path_shell_script(ls_path))
end

def custom_ls_path_shell_script(path)
  <<TXT
export LOGSTASH_PATH='#{path}'
export LOGSTASH_SOURCE=1
bundle install
bundle exec rake vendor
bundle exec rake travis_test
TXT
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