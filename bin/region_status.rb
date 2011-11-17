#/**
# * Copyright 2011 The Apache Software Foundation
# *
# * Licensed to the Apache Software Foundation (ASF) under one
# * or more contributor license agreements.  See the NOTICE file
# * distributed with this work for additional information
# * regarding copyright ownership.  The ASF licenses this file
# * to you under the Apache License, Version 2.0 (the
# * "License"); you may not use this file except in compliance
# * with the License.  You may obtain a copy of the License at
# *
# *     http://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# */

# View the current status of all regions on an HBase cluster.  This is
# predominantly used to determined if all the regions in META have been
# onlined yet on startup.
#
# To use this script, run:
#
#  ${HBASE_HOME}/bin/hbase org.jruby.Main region_status.rb [wait] [--table <table_name>]


require 'optparse'

usage = 'Usage : ./hbase org.jruby.Main region_status.rb [wait]' +
  '[--table <table_name>]\n'
OptionParser.new do |o|
  o.banner = usage
  o.on('-t', '--table TABLENAME', 'Only process TABLENAME') do |tablename|
    $tablename = tablename
  end
  o.on('-h', '--help', 'Display help message') { puts o; exit }
  o.parse!
end

SHOULD_WAIT = ARGV[0] == 'wait'
if ARGV[0] and not SHOULD_WAIT
  print usage
  exit 1
end


require 'java'

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.MasterNotRunningException
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.util.Writables

# disable debug logging on this script for clarity
log_level = org.apache.log4j.Level::ERROR
org.apache.log4j.Logger.getLogger("org.apache.zookeeper").setLevel(log_level)
org.apache.log4j.Logger.getLogger("org.apache.hadoop.hbase").setLevel(log_level)

config = HBaseConfiguration.create
config.set 'fs.default.name', config.get(HConstants::HBASE_DIR)

# wait until the master is running
admin = nil
while true
  begin
    admin = HBaseAdmin.new config
    break
  rescue MasterNotRunningException => e
    print 'Waiting for master to start...\n'
    sleep 1
  end
end

meta_count = 0
server_count = 0

# scan META to see how many regions we should have
if $tablename.nil?
  scan = Scan.new
else
  tableNameMetaPrefix = $tablename + HConstants::META_ROW_DELIMITER.chr
  scan = Scan.new(
    (tableNameMetaPrefix + HConstants::META_ROW_DELIMITER.chr).to_java_bytes
  )
end
scan.cache_blocks = false
scan.caching = 10
scan.setFilter(FirstKeyOnlyFilter.new)
INFO = 'info'.to_java_bytes
REGION_INFO = 'regioninfo'.to_java_bytes
scan.addColumn INFO, REGION_INFO
table = nil
iter = nil
while true
  begin
    table = HTable.new config, '.META.'.to_java_bytes
    scanner = table.getScanner(scan)
    iter = scanner.iterator
    break
  rescue IOException => ioe
    print "Exception trying to scan META: #{ioe}"
    sleep 1
  end
end
while iter.hasNext
  result = iter.next
  rowid = Bytes.toString(result.getRow())
  rowidStr = java.lang.String.new(rowid)
  if not $tablename.nil? and not rowidStr.startsWith(tableNameMetaPrefix)
    # Gone too far, break
    break
  end
  region = Writables.getHRegionInfo result.getValue(INFO, REGION_INFO)
  if not region.isOffline
    # only include regions that should be online
    meta_count += 1
  end
end
scanner.close
# If we're trying to see the status of all HBase tables, we need to include the
# -ROOT- & .META. tables, that are not included in our scan
if $tablename.nil?
  meta_count += 2
end

# query the master to see how many regions are on region servers
if not $tablename.nil?
  $tableq = HTable.new config, $tablename.to_java_bytes
end
while true
  if $tablename.nil?
    server_count = admin.getClusterStatus().getRegionsCount()
  else
    server_count = $tableq.getRegionsInfo().size()
  end
  print "Region Status: #{server_count} / #{meta_count}\n"
  if SHOULD_WAIT and server_count < meta_count
    #continue this loop until server & meta count match
    sleep 10
  else
    break
  end
end

exit server_count == meta_count ? 0 : 1
