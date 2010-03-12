require 'rubygems'
require 'rake'

unless defined?($TEST_CLUSTER)
  include Java

  # Set logging level to avoid verboseness
  org.apache.log4j.Logger.getRootLogger.setLevel(org.apache.log4j.Level::OFF)
  org.apache.log4j.Logger.getLogger("org.apache.zookeeper").setLevel(org.apache.log4j.Level::OFF)
  org.apache.log4j.Logger.getLogger("org.apache.hadoop.hdfs").setLevel(org.apache.log4j.Level::OFF)
  org.apache.log4j.Logger.getLogger("org.apache.hadoop.hbase").setLevel(org.apache.log4j.Level::OFF)
  org.apache.log4j.Logger.getLogger("org.apache.hadoop.ipc.HBaseServer").setLevel(org.apache.log4j.Level::OFF)

  java_import org.apache.hadoop.hbase.HBaseTestingUtility

  $TEST_CLUSTER = HBaseTestingUtility.new
  $TEST_CLUSTER.configuration.setInt("hbase.regionserver.msginterval", 100)
  $TEST_CLUSTER.configuration.setInt("hbase.client.pause", 250)
  $TEST_CLUSTER.configuration.setInt("hbase.client.retries.number", 6)
  $TEST_CLUSTER.startMiniCluster
  @own_cluster = true
end

require 'test_helper'

puts "Running tests..."

files = Dir[ File.dirname(__FILE__) + "/**/*_test.rb" ]
files.each do |file|
  begin
    load(file)
  rescue => e
    puts "ERROR: #{e}"
    raise
  end
end

Test::Unit::AutoRunner.run

puts "Done with tests! Shutting down the cluster..."
if @own_cluster
  $TEST_CLUSTER.shutdownMiniCluster
  java.lang.System.exit(0)
end
