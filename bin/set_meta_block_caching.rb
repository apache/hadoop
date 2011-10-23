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

# Set in_memory=true and blockcache=true on catalog tables.
# The .META. and -ROOT- tables can be created with caching and
# in_memory set to false.  You want them set to true so that
# these hot tables make it into cache.  To see if the
# .META. table has BLOCKCACHE set, in the shell do the following:
#
#   hbase> scan '-ROOT-'
#
# Look for the 'info' column family.  See if BLOCKCACHE => 'true'? 
# If not, run this script and it will set the value to true.
# Setting cache to 'true' will only take effect on region restart
# of if you close the .META. region -- *disruptive* -- and have
# it deploy elsewhere.  This script runs against an up and running
# hbase instance.
# 
# To see usage for this script, run: 
#
#  ${HBASE_HOME}/bin/hbase org.jruby.Main set_meta_block_caching.rb
#
include Java
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.HRegionInfo
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Delete
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.util.FSUtils
import org.apache.hadoop.hbase.util.Writables
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.commons.logging.LogFactory

# Name of this script
NAME = "set_meta_block_caching.rb"


# Print usage for this script
def usage
  puts 'Usage: %s.rb]' % NAME
  exit!
end

# Get configuration to use.
c = HBaseConfiguration.new()

# Set hadoop filesystem configuration using the hbase.rootdir.
# Otherwise, we'll always use localhost though the hbase.rootdir
# might be pointing at hdfs location.
c.set("fs.default.name", c.get(HConstants::HBASE_DIR))
fs = FileSystem.get(c)

# Get a logger and a metautils instance.
LOG = LogFactory.getLog(NAME)

# Check arguments
if ARGV.size > 0
  usage
end

# Clean mentions of table from .META.
# Scan the .META. and remove all lines that begin with tablename
metaTable = HTable.new(c, HConstants::ROOT_TABLE_NAME)
scan = Scan.new()
scan.addColumn(HConstants::CATALOG_FAMILY, HConstants::REGIONINFO_QUALIFIER);
scanner = metaTable.getScanner(scan)
while (result = scanner.next())
  rowid = Bytes.toString(result.getRow())
  LOG.info("Setting BLOCKCACHE and IN_MEMORY on: " + rowid);
  hriValue = result.getValue(HConstants::CATALOG_FAMILY, HConstants::REGIONINFO_QUALIFIER)
  hri = Writables.getHRegionInfo(hriValue)
  family = hri.getTableDesc().getFamily(HConstants::CATALOG_FAMILY)
  family.setBlockCacheEnabled(true)
  family.setInMemory(true)
  p = Put.new(result.getRow())
  p.add(HConstants::CATALOG_FAMILY, HConstants::REGIONINFO_QUALIFIER, Writables.getBytes(hri));
  metaTable.put(p)
end
scanner.close()
