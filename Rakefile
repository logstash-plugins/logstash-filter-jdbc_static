# encoding: utf-8
require "jars/installer"
require "fileutils"
require "logstash/devutils/rake"
require "rspec/core/rake_task"

task :default do
  system('rake -vT')
end

task :vendor do
  exit(1) unless system './gradlew check vendor'
end

task :clean do
  ["vendor/jar-dependencies", "Gemfile.lock"].each do |p|
    FileUtils.rm_rf(p)
  end
end

desc "Write gradle.properties"
task :write_gradle_properties do
  delete_create_gradle_properties
end

RSpec::Core::RakeTask.new(:spec)

task :travis_test do
  # bundle install done already
  delete_create_gradle_properties
  Rake::Task["vendor"].execute
  puts "-------------------> vendored jdbc_static jar via rake"
  Rake::Task["spec"].execute
end

desc "Run full check with custom Logstash path" # e.g. rake custom_ls_check[/elastic/tmp/logstash-5.5.3]
task(:custom_ls_check, [:ls_dir] => [:clean]) do |task, args|
  ls_path = args[:ls_dir]
  system(custom_ls_path_shell_script(ls_path))
end

# should invoke the same workflow as travis
def custom_ls_path_shell_script(path)
  <<TXT
export LOGSTASH_PATH='#{path}'
export LOGSTASH_SOURCE=1
bundle install
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