# encoding: utf-8
require "childprocess"

ENV["TEST_DEBUG"] = "true"
java.lang.System.setProperty("ls.logs", "console")

GEM_BASE_DIR = ::File.expand_path("../../..", __FILE__)
BASE_DERBY_DIR = ::File.join(GEM_BASE_DIR, "vendor", "lib")

module ServerProcessHelpers
  def self.jdbc_static_start_derby_server()
    # client_out = Stud::Temporary.file
    # client_out.sync
    ChildProcess.posix_spawn = true
    cmd = ["java",  "-jar", "#{BASE_DERBY_DIR}/derbyrun.jar", "server",  "start"]
    process = ChildProcess.build(*cmd)
    # process.duplex = true
    # process.io.stdout = process.io.stderr = client_out
    process.start

    sleep(0.1)
  end

  def self.jdbc_static_stop_derby_server(test_db)
    cmd = ["java",  "-jar", "#{BASE_DERBY_DIR}/derbyrun.jar", "server",  "shutdown"]
    process = ChildProcess.build(*cmd)
    ChildProcess.posix_spawn = true
    process.start
    process.wait
    `rm -rf #{::File.join(GEM_BASE_DIR, test_db)}`
  end
end
