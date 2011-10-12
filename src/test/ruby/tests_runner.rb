#
# Copyright 2010 The Apache Software Foundation
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

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

if !Test::Unit::AutoRunner.run
  raise "Shell unit tests failed. Check output file for details."
end

puts "Done with tests! Shutting down the cluster..."
if @own_cluster
  $TEST_CLUSTER.shutdownMiniCluster
  java.lang.System.exit(0)
end
